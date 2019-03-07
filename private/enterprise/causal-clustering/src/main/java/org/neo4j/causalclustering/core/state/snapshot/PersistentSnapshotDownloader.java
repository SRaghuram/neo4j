/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.kernel.internal.DatabaseHealth;
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
    private final Supplier<DatabaseHealth> dbHealth;
    private final Monitor monitor;
    private volatile State state;
    private volatile boolean stopped;

    PersistentSnapshotDownloader( CatchupAddressProvider addressProvider, CommandApplicationProcess applicationProcess, Suspendable auxiliaryServices,
            DatabaseService databaseService, CoreDownloader downloader, CoreSnapshotService snapshotService, Log log, TimeoutStrategy backoffStrategy,
            Supplier<DatabaseHealth> dbHealth, Monitors monitors )
    {
        this.applicationProcess = applicationProcess;
        this.addressProvider = addressProvider;
        this.auxiliaryServices = auxiliaryServices;
        this.databaseService = databaseService;
        this.downloader = downloader;
        this.snapshotService = snapshotService;
        this.log = log;
        this.backoffStrategy = backoffStrategy;
        this.dbHealth = dbHealth;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.state = State.INITIATED;
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

            Optional<CoreSnapshot> snapshotOptional = downloadSnapshotAndStores();

            if ( snapshotOptional.isPresent() )
            {
                CoreSnapshot snapshot = snapshotOptional.get();

                snapshotService.installSnapshot( snapshot );
                log.info( "Core snapshot installed: " + snapshot );

                /* Starting the databases will invoke the commit process factory in the EnterpriseCoreEditionModule, which has important side-effects. */
                log.info( "Starting local databases" );
                databaseService.start();
                auxiliaryServices.enable();
            }
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
            dbHealth.get().panic( e );
        }
        finally
        {
            applicationProcess.resumeApplier( OPERATION_NAME );
            monitor.downloadSnapshotComplete();
            state = State.COMPLETED;
        }
    }

    /**
     * Attempt to download a {@link CoreSnapshot} and bring the database up-to-date by pulling transactions
     * or doing a store-copy. Method will retry until a snapshot is downloaded and the database is caught up
     * or until the downloader is {@link #stop() stopped}.
     *
     * @return an optional containing snapshot when it was downloaded and the database is up-to-date.
     * An empty optional when this downloader was stopped.
     */
    private Optional<CoreSnapshot> downloadSnapshotAndStores() throws IOException, DatabaseShutdownException, InterruptedException
    {
        TimeoutStrategy.Timeout backoff = backoffStrategy.newTimeout();
        while ( true )
        {
            // check if we were stopped before doing a potentially long running catchup operation
            if ( stopped )
            {
                log.info( "Persistent snapshot downloader was stopped before download succeeded" );
                return Optional.empty();
            }

            Optional<CoreSnapshot> optionalSnapshot = downloader.downloadSnapshotAndStores( addressProvider );

            // check if we were stopped after catchup to avoid starting databases after a shutdown was requested
            if ( stopped )
            {
                log.info( "Persistent snapshot downloader was stopped after a download attempt: " + optionalSnapshot );
                return Optional.empty();
            }

            if ( optionalSnapshot.isPresent() )
            {
                return optionalSnapshot;
            }
            else
            {
                Thread.sleep( backoff.getAndIncrement() );
            }
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
        stopped = true;

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
