/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.dbms.ReplicatedDatabaseEventService;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.collection.Dependencies;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;

public class PersistentSnapshotDownloader implements Runnable
{
    public interface Monitor
    {
        void startedDownloadingSnapshot( NamedDatabaseId namedDatabaseId );

        void downloadSnapshotComplete( NamedDatabaseId namedDatabaseId );
    }

    static final String DOWNLOAD_SNAPSHOT = "download of snapshot";
    static final String SHUTDOWN = "shutdown";

    private final StoreDownloadContext context;
    private final CommandApplicationProcess applicationProcess;
    private final CatchupAddressProvider addressProvider;
    private final CoreDownloader downloader;
    private final CoreSnapshotService snapshotService;
    private final ReplicatedDatabaseEventService databaseEventService;
    private final Log log;
    private final TimeoutStrategy backoffStrategy;
    private final DatabasePanicker panicker;
    private final Monitor monitor;
    private volatile State state;
    private volatile boolean stopped;

    PersistentSnapshotDownloader( CatchupAddressProvider addressProvider, CommandApplicationProcess applicationProcess, CoreDownloader downloader,
            CoreSnapshotService snapshotService, ReplicatedDatabaseEventService databaseEventService, StoreDownloadContext context, Log log,
            TimeoutStrategy backoffStrategy, DatabasePanicker panicker, Monitors monitors )
    {
        this.applicationProcess = applicationProcess;
        this.addressProvider = addressProvider;
        this.downloader = downloader;
        this.snapshotService = snapshotService;
        this.databaseEventService = databaseEventService;
        this.context = context;
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

        var databaseId = context.databaseId();
        try
        {
            monitor.startedDownloadingSnapshot( databaseId );
            applicationProcess.pauseApplier( DOWNLOAD_SNAPSHOT );

            log.info( "Stopping kernel before store copy" );
            var stoppedKernel = context.stopForStoreCopy();

            boolean incomplete = false;
            Optional<CoreSnapshot> snapshot = downloadSnapshotAndStore( context );

            if ( snapshot.isPresent() )
            {
                log.info( "Core snapshot downloaded: %s", snapshot.get() );
            }
            else
            {
                log.warn( "Core snapshot could not be downloaded" );
                incomplete = true;
            }

            if ( incomplete )
            {
                log.warn( "Not starting kernel after failed store copy" );
            }
            else if ( stopped )
            {
                log.warn( "Not starting kernel after store copy because database has been requested to stop" );
            }
            else
            {
                snapshotService.installSnapshot( snapshot.get() );

                /* Starting the database will invoke the CoreCommitProcessFactory which has important side-effects. */
                log.info( "Attempting kernel start after store copy" );
                boolean reconcilerTriggered = stoppedKernel.release();

                if ( !reconcilerTriggered )
                {
                    log.info( "Reconciler could not be triggered at this time. This is expected during bootstrapping." );
                }
                else if ( !context.kernelDatabase().isStarted() )
                {
                    log.warn( "Kernel did not start properly after the store copy. This might be because of unexpected errors or normal early-exit paths." );
                }
                else
                {
                    log.info( "Kernel started after store copy" );
                    notifyStoreCopied( context.kernelDatabase() );
                }
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
            applicationProcess.resumeApplier( DOWNLOAD_SNAPSHOT );
            monitor.downloadSnapshotComplete( databaseId );
            state = State.COMPLETED;
        }
    }

    private void notifyStoreCopied( Database database )
    {
        Dependencies dependencyResolver = database.getDependencyResolver();
        TransactionIdStore txIdStore = dependencyResolver.resolveDependency( TransactionIdStore.class );
        long lastCommittedTransactionId = txIdStore.getLastCommittedTransactionId();
        assert lastCommittedTransactionId == txIdStore.getLastClosedTransactionId();
        databaseEventService.getDatabaseEventDispatch( context.databaseId() ).fireStoreReplaced( lastCommittedTransactionId );
    }

    /**
     * Attempt to download a {@link CoreSnapshot} and bring the database up-to-date by pulling transactions
     * or doing a store-copy. Method will retry until a snapshot is downloaded and the database is caught up
     * or until the downloader is {@link #stop() stopped}.
     *
     * @return an optional containing snapshot when it was downloaded and the database is up-to-date.
     * An empty optional when this downloader was stopped.
     */
    private Optional<CoreSnapshot> downloadSnapshotAndStore( StoreDownloadContext context ) throws IOException, DatabaseShutdownException, InterruptedException
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

            Optional<CoreSnapshot> optionalSnapshot = downloader.downloadSnapshotAndStore( context, addressProvider );

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
        applicationProcess.pauseApplier( SHUTDOWN );
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
