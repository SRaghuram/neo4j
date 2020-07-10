/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.UpstreamStrategyBasedAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.util.VisibleForTesting;

import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static com.neo4j.causalclustering.readreplica.CatchupProcessManager.Timers.TX_PULLER_TIMER;

/**
 * This class is responsible for aggregating a number of {@link CatchupPollingProcess} instances and pulling transactions for
 * each database present on this machine. These pull operations are issued on a fixed interval and take place in parallel.
 *
 * If the necessary transactions are not remotely available then a fresh copy of the
 * entire store will be pulled down.
 */
// TODO: Get rid of this aggregation, since we no longer have any need to aggregate.
public class CatchupProcessManager extends SafeLifecycle
{
    public enum Timers implements TimerService.TimerName
    {
        TX_PULLER_TIMER
    }

    private final TopologyService topologyService;
    private final CatchupClientFactory catchupClient;
    private final UpstreamDatabaseStrategySelector selectionStrategyPipeline;
    private final TimerService timerService;
    private final long txPullIntervalMillis;
    private final CommandIndexTracker commandIndexTracker;
    private final Executor executor;
    private final ReadReplicaDatabaseContext databaseContext;
    private final LogProvider logProvider;
    private final Log log;
    private final Config config;
    private final CatchupComponentsRepository catchupComponents;
    private final DatabasePanicker panicker;
    private final ReplicatedDatabaseEventDispatch databaseEventDispatch;
    private final PageCacheTracer pageCacheTracer;

    private CatchupPollingProcess catchupProcess;
    private LifeSupport txPulling;
    private volatile boolean isPanicked;
    private volatile boolean txPullingPaused;
    private Timer timer;

    CatchupProcessManager( Executor executor, CatchupComponentsRepository catchupComponents, ReadReplicaDatabaseContext databaseContext,
            DatabasePanicker panicker, TopologyService topologyService, CatchupClientFactory catchUpClient,
            UpstreamDatabaseStrategySelector selectionStrategyPipeline, TimerService timerService, CommandIndexTracker commandIndexTracker,
            LogProvider logProvider, Config config, ReplicatedDatabaseEventDispatch databaseEventDispatch, PageCacheTracer pageCacheTracer )
    {
        this.logProvider = logProvider;
        this.log = logProvider.getLog( this.getClass() );
        this.config = config;
        this.commandIndexTracker = commandIndexTracker;
        this.timerService = timerService;
        this.executor = executor;
        this.catchupComponents = catchupComponents;
        this.databaseContext = databaseContext;
        this.panicker = panicker;
        this.topologyService = topologyService;
        this.catchupClient = catchUpClient;
        this.selectionStrategyPipeline = selectionStrategyPipeline;
        this.txPullIntervalMillis = config.get( CausalClusteringSettings.pull_interval ).toMillis();
        this.databaseEventDispatch = databaseEventDispatch;
        this.pageCacheTracer = pageCacheTracer;
        this.isPanicked = false;
    }

    @Override
    public void start0()
    {
        log.info( "Starting " + this.getClass().getSimpleName() );
        txPulling = new LifeSupport();
        catchupProcess = createCatchupProcess( databaseContext );
        txPulling.start();
        initTimer();
        txPullingPaused = false;
    }

    @Override
    public void stop0()
    {
        log.info( "Shutting down " + this.getClass().getSimpleName() );
        timer.kill( Timer.CancelMode.SYNC_WAIT );
        txPulling.stop();
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
        return true;
    }

    public synchronized void panic( Throwable e )
    {
        log.error( "Unexpected issue in catchup process. No more catchup requests will be scheduled.", e );
        panicker.panic( e );
        isPanicked = true;
    }

    private CatchupPollingProcess createCatchupProcess( ReadReplicaDatabaseContext databaseContext )
    {
        CatchupComponents dbCatchupComponents = catchupComponents.componentsFor( databaseContext.databaseId() ).orElseThrow(
                () -> new IllegalArgumentException( String.format( "No StoreCopyProcess instance exists for database %s.", databaseContext.databaseId() ) ) );

        // TODO: We can do better than this. Core already exposes its commit process. Why not RR.
        Supplier<TransactionCommitProcess> writableCommitProcess = () -> new TransactionRepresentationCommitProcess(
                databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( TransactionAppender.class ),
                databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( StorageEngine.class ) );

        int maxBatchSize = config.get( CausalClusteringInternalSettings.read_replica_transaction_applier_batch_size );
        BatchingTxApplier batchingTxApplier = new BatchingTxApplier( maxBatchSize,
                () -> databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( TransactionIdStore.class ), writableCommitProcess,
                databaseContext.monitors(), databaseContext.kernelDatabase().getVersionContextSupplier(), commandIndexTracker,
                logProvider, databaseEventDispatch, pageCacheTracer );

        CatchupPollingProcess catchupProcess = new CatchupPollingProcess( executor, databaseContext, catchupClient,
                batchingTxApplier, databaseEventDispatch, dbCatchupComponents.storeCopyProcess(), logProvider, this::panic,
                new UpstreamStrategyBasedAddressProvider( topologyService, selectionStrategyPipeline ) );

        txPulling.add( batchingTxApplier );
        txPulling.add( catchupProcess );
        return catchupProcess;
    }

    /**
     * Time to catchup, thrusters to maximum!
     */
    private void onTimeout() throws Exception
    {
        if ( !txPullingPaused )
        {
            catchupProcess.tick().get();
        }

        if ( !isPanicked )
        {
            timer.reset();
        }
    }

    private boolean isCatchupProcessAvailableForPausing()
    {
        return catchupProcess != null && !catchupProcess.isStoryCopy();
    }

    @VisibleForTesting
    void setCatchupProcess( CatchupPollingProcess catchupProcess )
    {
        this.catchupProcess = catchupProcess;
    }

    @VisibleForTesting
    public CatchupPollingProcess getCatchupProcess()
    {
        return catchupProcess;
    }

    void initTimer()
    {
        timer = timerService.create( TX_PULLER_TIMER, Group.PULL_UPDATES, timeout -> onTimeout() );
        timer.set( fixedTimeout( txPullIntervalMillis, TimeUnit.MILLISECONDS ) );
    }
}
