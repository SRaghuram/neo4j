/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.helper.Suspendable;
import com.neo4j.causalclustering.helper.TimeoutStrategy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;

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
    private final ClusteredDatabaseManager<?> databaseManager;
    private final CoreDownloader downloader;
    private final CoreSnapshotService snapshotService;
    private final Log log;
    private final TimeoutStrategy backoffStrategy;
    private final Panicker panicker;
    private final Monitor monitor;
    private volatile State state;
    private volatile boolean stopped;

    PersistentSnapshotDownloader( CatchupAddressProvider addressProvider, CommandApplicationProcess applicationProcess, Suspendable auxiliaryServices,
            ClusteredDatabaseManager<?> databaseManager, CoreDownloader downloader, CoreSnapshotService snapshotService,
            Log log, TimeoutStrategy backoffStrategy, Panicker panicker, Monitors monitors )
    {
        this.applicationProcess = applicationProcess;
        this.addressProvider = addressProvider;
        this.auxiliaryServices = auxiliaryServices;
        this.databaseManager = databaseManager;
        this.downloader = downloader;
        this.snapshotService = snapshotService;
        this.log = log;
        this.backoffStrategy = backoffStrategy;
        this.panicker = panicker;
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
            databaseManager.stopForStoreCopy();

            // iteration over all databases should go away once we have separate database lifecycles
            // each database will then download its snapshot and store independently from others

            var databases = new HashMap<>( databaseManager.registeredDatabases() );
            var snapshots = new HashMap<String,CoreSnapshot>();

            boolean incomplete = false;
            for ( ClusteredDatabaseContext db : databases.values() )
            {
                Optional<CoreSnapshot> snapshot = downloadSnapshotAndStore( db );
                String databaseName = db.databaseName();

                if ( snapshot.isPresent() )
                {
                    snapshots.put( databaseName, snapshot.get() );
                    log.info( format( "Core snapshot for database '%s' downloaded: %s", databaseName, snapshot.get() ) );
                }
                else
                {
                    log.warn( format( "Core snapshot for database '%s' could not be downloaded", databaseName ) );
                    incomplete = true;
                }
            }

            if ( incomplete || stopped )
            {
                log.warn( "Not starting databases after store copy." );
            }
            else
            {
                /* Temporary until raft groups are separated. */
                snapshotService.installSnapshots( snapshots );

                /* Starting the databases will invoke the commit process factory in the CoreEditionModule, which has important side-effects. */
                log.info( "Starting local databases" );
                databaseManager.start();
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
            panicker.panic( e );
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
    private Optional<CoreSnapshot> downloadSnapshotAndStore( ClusteredDatabaseContext db ) throws IOException, DatabaseShutdownException, InterruptedException
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

            Optional<CoreSnapshot> optionalSnapshot = downloader.downloadSnapshotAndStore( db, addressProvider );

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
