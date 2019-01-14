/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import java.util.Optional;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.error_handling.Panicker;
import org.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

public class PersistentSnapshotDownloader implements Runnable
{
    public interface Monitor
    {
        void startedDownloadingSnapshot();

        void downloadSnapshotComplete();
    }

    static final String OPERATION_NAME = "download of snapshot";

    private final CommandApplicationProcess applicationProcess;
    private final CatchupAddressProvider addressProvider;
    private final Suspendable auxiliaryServices;
    private final DatabaseService databaseService;
    private final CoreDownloader downloader;
    private final CoreSnapshotService snapshotService;
    private final Log log;
    private final TimeoutStrategy backoffStrategy;
    private final Panicker panicker;
    private final Monitor monitor;
    private volatile State state;
    private volatile boolean keepRunning;

    PersistentSnapshotDownloader( CatchupAddressProvider addressProvider, CommandApplicationProcess applicationProcess, Suspendable auxiliaryServices,
            DatabaseService databaseService, CoreDownloader downloader, CoreSnapshotService snapshotService, Log log, TimeoutStrategy backoffStrategy,
            Panicker panicker, Monitors monitors )
    {
        this.applicationProcess = applicationProcess;
        this.addressProvider = addressProvider;
        this.auxiliaryServices = auxiliaryServices;
        this.databaseService = databaseService;
        this.downloader = downloader;
        this.snapshotService = snapshotService;
        this.log = log;
        this.backoffStrategy = backoffStrategy;
        this.panicker = panicker;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.state = State.INITIATED;
        this.keepRunning = true;
    }

    private enum State
    {
        INITIATED,
        RUNNING,
        COMPLETED
    }

    @Override
    public void run()
    {
        if ( !moveToRunningState() )
        {
            return;
        }

        try
        {
            monitor.startedDownloadingSnapshot();
            applicationProcess.pauseApplier( OPERATION_NAME );

            auxiliaryServices.disable();
            databaseService.stopForStoreCopy();

            TimeoutStrategy.Timeout backoff = backoffStrategy.newTimeout();
            Optional<CoreSnapshot> snapshot;
            while ( keepRunning )
            {
                snapshot = downloader.downloadSnapshotAndStores( addressProvider );
                if ( snapshot.isPresent() )
                {
                    snapshotService.installSnapshot( snapshot.get() );
                    log.info( "Core snapshot installed: " + snapshot );
                    keepRunning = false;
                }
                else
                {
                    Thread.sleep( backoff.getAndIncrement() );
                }
            }

            /* Starting the databases will invoke the commit process factory in the EnterpriseCoreEditionModule, which has important side-effects. */
            log.info( "Starting local databases" );
            databaseService.start();
            auxiliaryServices.enable();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            log.warn( "Persistent snapshot downloader was interrupted" );
        }
        catch ( DatabaseShutdownException e )
        {
            log.warn( "Store copy aborted due to shut down", e );
        }
        catch ( Throwable e )
        {
            log.error( "Unrecoverable error during store copy", e );
            panicker.panic( e );
        }
        finally
        {
            applicationProcess.resumeApplier( OPERATION_NAME );
            monitor.downloadSnapshotComplete();
            state = State.COMPLETED;
        }
    }

    private synchronized boolean moveToRunningState()
    {
        if ( state != State.INITIATED )
        {
            return false;
        }
        else
        {
            state = State.RUNNING;
            return true;
        }
    }

    void stop() throws InterruptedException
    {
        this.keepRunning = false;

        while ( !hasCompleted() )
        {
            Thread.sleep( 100 );
        }
    }

    boolean hasCompleted()
    {
        return state == State.COMPLETED;
    }
}
