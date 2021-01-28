/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.error_handling.DatabasePanicEvent;
import com.neo4j.causalclustering.error_handling.DatabasePanicReason;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.readreplica.CatchupProcessFactory.CatchupProcessComponents;
import com.neo4j.configuration.CausalClusteringSettings;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.util.VisibleForTesting;

import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static com.neo4j.causalclustering.readreplica.CatchupProcessManager.Timers.TX_PULLER_TIMER;

/**
 * This class is responsible for aggregating a number of {@link CatchupPollingProcess} instances and pulling transactions for each database present on this
 * machine. These pull operations are issued on a fixed interval and take place in parallel.
 * <p>
 * If the necessary transactions are not remotely available then a fresh copy of the entire store will be pulled down.
 */
// TODO: Get rid of this aggregation, since we no longer have any need to aggregate.
public class CatchupProcessManager extends SafeLifecycle
{
    public enum Timers implements TimerService.TimerName
    {
        TX_PULLER_TIMER
    }

    private final TimerService timerService;
    private final long txPullIntervalMillis;
    private final ReadReplicaDatabaseContext databaseContext;
    private final Log log;
    private final Panicker panicker;
    private CatchupProcessComponents catchupProcessComponents;
    private volatile boolean isPanicked;
    private volatile boolean txPullingPaused;
    private Timer timer;
    private final CatchupProcessFactory catchupProcessFactory;

    CatchupProcessManager( ReadReplicaDatabaseContext databaseContext, Panicker panicker, TimerService timerService, LogProvider logProvider, Config config,
            CatchupProcessFactory catchupProcessFactory )
    {
        this.log = logProvider.getLog( this.getClass() );
        this.timerService = timerService;
        this.databaseContext = databaseContext;
        this.panicker = panicker;
        this.txPullIntervalMillis = config.get( CausalClusteringSettings.pull_interval ).toMillis();
        this.catchupProcessFactory = catchupProcessFactory;
        this.isPanicked = false;
    }

    @Override
    public void start0()
    {
        log.info( "Starting " + this.getClass().getSimpleName() );
        catchupProcessComponents = catchupProcessFactory.create( databaseContext );
        catchupProcessComponents.start();
        initTimer();
        txPullingPaused = false;
    }

    @Override
    public void stop0()
    {
        log.info( "Shutting down " + this.getClass().getSimpleName() );
        catchupProcessComponents.shutdown();
        timer.kill( Timer.CancelMode.SYNC_WAIT );
        txPullingPaused = true;
    }

    public boolean pauseCatchupProcess()
    {
        if ( txPullingPaused )
        {
            return false;
        }
        if ( !isCatchupProcessAvailableForPausing() )
        {
            throw new IllegalStateException( "Catchup process can't be stopped" );
        }
        log.info( "Pausing transaction pulling" );
        txPullingPaused = true;
        return true;
    }

    public boolean resumeCatchupProcess()
    {
        if ( !txPullingPaused )
        {
            return false;
        }
        txPullingPaused = false;
        log.info( "Resuming transaction pulling" );
        return true;
    }

    @VisibleForTesting
    synchronized void panicDatabase( DatabasePanicReason reason, Throwable e )
    {
        log.error( "Unexpected issue in catchup process. No more catchup requests will be scheduled.", e );
        panicker.panic( new DatabasePanicEvent( databaseContext.databaseId(), reason, e ) );
        isPanicked = true;
    }

    /**
     * Time to catchup, thrusters to maximum!
     */
    private void onTimeout()
    {
        if ( !txPullingPaused )
        {
            catchupProcessComponents.catchupProcess().tick();
        }

        if ( !isPanicked )
        {
            timer.reset();
        }
    }

    private boolean isCatchupProcessAvailableForPausing()
    {
        return catchupProcessComponents != null && !catchupProcessComponents.catchupProcess().isCopyingStore();
    }

    @VisibleForTesting
    public CatchupPollingProcess getCatchupProcess()
    {
        return catchupProcessComponents.catchupProcess();
    }

    private void initTimer()
    {
        timer = timerService.create( TX_PULLER_TIMER, Group.PULL_UPDATES, timeout -> onTimeout() );
        timer.set( fixedTimeout( txPullIntervalMillis, TimeUnit.MILLISECONDS ) );
    }
}
