/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.error_handling.DatabasePanicEvent;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.dbms.ReplicatedDatabaseEventService;

import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.kernel.database.Database;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;

import static com.neo4j.causalclustering.error_handling.DatabasePanicReason.SNAPSHOT_FAILED;

class PersistentSnapshotDownloader implements Runnable
{

    static final String DOWNLOAD_SNAPSHOT = "download of snapshot";
    static final String SHUTDOWN = "shutdown";

    private final StoreDownloadContext context;
    private final CommandApplicationProcess applicationProcess;
    private final CatchupAddressProvider addressProvider;
    private final CoreDownloader downloader;
    private final CoreSnapshotService snapshotService;
    private final ReplicatedDatabaseEventService databaseEventService;
    private final Log log;
    private final TimeoutStrategy strategy;
    private final Panicker panicker;
    private final CoreSnapshotMonitor coreSnapshotMonitor;
    private volatile State state;
    private volatile boolean stopped;

    PersistentSnapshotDownloader( CatchupAddressProvider addressProvider, CommandApplicationProcess applicationProcess, CoreDownloader downloader,
                                  CoreSnapshotService snapshotService, ReplicatedDatabaseEventService databaseEventService, StoreDownloadContext context,
                                  Log log, TimeoutStrategy strategy, Panicker panicker, Monitors monitors )
    {
        this.applicationProcess = applicationProcess;
        this.addressProvider = addressProvider;
        this.downloader = downloader;
        this.snapshotService = snapshotService;
        this.databaseEventService = databaseEventService;
        this.context = context;
        this.log = log;
        this.strategy = strategy;
        this.panicker = panicker;
        this.coreSnapshotMonitor = monitors.newMonitor( CoreSnapshotMonitor.class );
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
            coreSnapshotMonitor.startedDownloadingSnapshot( databaseId );
            applicationProcess.pauseApplier( DOWNLOAD_SNAPSHOT );

            log.info( "Stopping kernel before store copy" );
            var stoppedKernel = context.stopForStoreCopy();

            var snapshot = downloadSnapshotAndStore( context );
            log.info( "Core snapshot downloaded: %s", snapshot );

            if ( stopped )
            {
                log.warn( "Not starting kernel after store copy because database has been requested to stop" );
            }
            else
            {
                snapshotService.installSnapshot( snapshot );

                /* Starting the database will invoke the CoreCommitProcessFactory which has important side-effects. */
                log.info( "Attempting kernel start after store copy" );
                var reconcilerTriggered = stoppedKernel.release();

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
        catch ( SnapshotFailedException e )
        {
            log.warn( "Core snapshot could not be downloaded" );
            log.warn( "Not starting kernel after failed store copy" );
            if ( e.status() == SnapshotFailedException.Status.UNRECOVERABLE )
            {
                log.error( "Unrecoverable error when downloading core snapshot and store.", e );
                panicker.panic( new DatabasePanicEvent( context.databaseId(), SNAPSHOT_FAILED, e ) );
            }
            else
            {
                log.warn( "Unable to download core snapshot", e );
            }
        }
        catch ( Throwable e )
        {
            log.error( "Unrecoverable error when downloading core snapshot and store.", e );
            panicker.panic( new DatabasePanicEvent( context.databaseId(), SNAPSHOT_FAILED, e ) );
        }
        finally
        {
            applicationProcess.resumeApplier( DOWNLOAD_SNAPSHOT );
            coreSnapshotMonitor.downloadSnapshotComplete( databaseId );
            state = State.COMPLETED;
        }
    }

    private void notifyStoreCopied( Database database )
    {
        var dependencyResolver = database.getDependencyResolver();
        var txIdStore = dependencyResolver.resolveDependency( TransactionIdStore.class );
        var lastCommittedTransactionId = txIdStore.getLastCommittedTransactionId();
        assert lastCommittedTransactionId == txIdStore.getLastClosedTransactionId();
        databaseEventService.getDatabaseEventDispatch( context.databaseId() ).fireStoreReplaced( lastCommittedTransactionId );
    }

    /**
     * Attempt to download a {@link CoreSnapshot} and bring the database up-to-date by pulling transactions or doing a store-copy. Method will retry until a
     * snapshot is downloaded and the database is caught up or until the downloader is {@link #stop() stopped}.
     *
     * @return an optional containing snapshot when it was downloaded and the database is up-to-date. An empty optional when this downloader was stopped.
     */
    private CoreSnapshot downloadSnapshotAndStore( StoreDownloadContext context ) throws SnapshotFailedException, InterruptedException
    {
        var backoff = strategy.newTimeout();
        while ( true )
        {
            // check if we were stopped before doing a potentially long running catchup operation
            if ( stopped )
            {
                log.info( "Persistent snapshot downloader was stopped before download succeeded" );
                throw new SnapshotFailedException( "Persistent snapshot downloader was stopped before download succeeded",
                        SnapshotFailedException.Status.TERMINAL );
            }

            try
            {
                return downloader.downloadSnapshotAndStore( context, addressProvider );
            }
            catch ( SnapshotFailedException e )
            {
                if ( e.status() != SnapshotFailedException.Status.RETRYABLE )
                {
                    throw e;
                }
            }
            Thread.sleep( backoff.getAndIncrement() );
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
