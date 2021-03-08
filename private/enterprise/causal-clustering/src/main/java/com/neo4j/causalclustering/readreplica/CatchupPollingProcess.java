/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.tx.PullRequestMonitor;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.error_handling.DatabasePanicEvent;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.readreplica.tx.AsyncTaskEventHandler;
import com.neo4j.causalclustering.readreplica.tx.BatchinTxApplierFactory;
import com.neo4j.causalclustering.readreplica.tx.BatchingTxApplier;
import com.neo4j.causalclustering.readreplica.tx.CancelledPullUpdatesJobException;
import com.neo4j.causalclustering.readreplica.tx.PullUpdatesJob;
import com.neo4j.dbms.ClusterInternalDbmsOperator.StoreCopyHandle;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.util.concurrent.BinaryLatch;

import static com.neo4j.causalclustering.error_handling.DatabasePanicReason.CATCHUP_FAILED;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.CANCELLED;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.PANIC;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.PAUSED;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.STORE_COPYING;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.TX_PULLING;
import static org.neo4j.util.Preconditions.checkState;

/**
 * This class is responsible for pulling transactions from a core server and queuing
 * them to be applied with the {@link BatchingTxApplier}.
 */
public class CatchupPollingProcess extends LifecycleAdapter
{
    protected enum State
    {
        TX_PULLING,
        STORE_COPYING,
        PANIC,
        CANCELLED,
        PAUSED
    }

    private final long maxQueueSize;
    private final long applyBatchSize;
    private final ReadReplicaDatabaseContext databaseContext;
    private final BatchinTxApplierFactory applierFactory;
    private final CatchupAddressProvider upstreamProvider;
    private final Log log;
    private final StoreCopyProcess storeCopyProcess;
    private final CatchupClientFactory catchUpClient;
    private final Panicker panicker;
    private final ReplicatedDatabaseEventDispatch databaseEventDispatch;
    private final PullRequestMonitor pullRequestMonitor;

    private volatile BatchingTxApplier applier;
    private volatile State state = TX_PULLING;
    private CompletableFuture<Boolean> upToDateFuture; // we are up-to-date when we are successfully pulling
    private StoreCopyHandle storeCopyHandle;

    CatchupPollingProcess( long maxQueueSize, long applyBatchSize, ReadReplicaDatabaseContext databaseContext, CatchupClientFactory catchUpClient,
            BatchinTxApplierFactory applierFactory, ReplicatedDatabaseEventDispatch databaseEventDispatch, StoreCopyProcess storeCopyProcess,
            LogProvider logProvider, Panicker panicker, CatchupAddressProvider upstreamProvider )
    {
        this.maxQueueSize = maxQueueSize;
        this.applyBatchSize = applyBatchSize;
        this.databaseContext = databaseContext;
        this.applierFactory = applierFactory;
        this.upstreamProvider = upstreamProvider;
        this.catchUpClient = catchUpClient;
        this.databaseEventDispatch = databaseEventDispatch;
        this.pullRequestMonitor = databaseContext.monitors().newMonitor( PullRequestMonitor.class );
        this.storeCopyProcess = storeCopyProcess;
        this.log = logProvider.getLog( getClass() );
        this.panicker = panicker;
    }

    @Override
    public synchronized void start()
    {
        log.debug( "Starting catchup polling process %s", this );
        state = TX_PULLING;
        upToDateFuture = new CompletableFuture<>();
        applier = applierFactory.create();
    }

    public CompletableFuture<Boolean> upToDateFuture()
    {
        log.debug( "Returning an up-to-date future %s", upToDateFuture );
        return upToDateFuture;
    }

    @Override
    public synchronized void stop()
    {
        log.debug( "Stopping catchup polling process %s", this );
        state = CANCELLED;
    }

    @VisibleForTesting
    public State state()
    {
        return state;
    }

    boolean isCopyingStore()
    {
        return state == STORE_COPYING;
    }

    /**
     * Time to catchup! //TODO: Fix error handling further down the stack to bubble up to this level rather than panicking, as that will not complete the
     * future.
     */
    public void tick()
    {
        try
        {
            switch ( state )
            {
            case TX_PULLING:
                pullTransactions();
                break;
            case STORE_COPYING:
                copyStore();
                break;
            case CANCELLED:
            case PANIC:
            case PAUSED:
                    break;
            default:
                throw new IllegalStateException( "Tried to execute catchup but was in state " + state );
            }
        }
        catch ( Throwable e )
        {
            log.error( "Polling process failed", e );
            panic( e );
        }
    }

    public synchronized boolean pause()
    {
        if ( state == CANCELLED ||
             state == PANIC ||
             state == STORE_COPYING )
        {
            throw new IllegalStateException( "Catchup process can't be paused" );
        }
        if ( state == PAUSED )
        {
            return false;
        }
        state = PAUSED;
        log.info( "Pausing transaction pulling" );
        return true;
    }

    public synchronized boolean resume()
    {
        if ( state == CANCELLED ||
             state == PANIC )
        {
            throw new IllegalStateException( "Catchup process can't be resumed" );
        }
        if ( state != PAUSED  )
        {
            return false;
        }
        state = TX_PULLING;
        log.info( "Resuming transaction pulling" );
        return true;
    }

    public boolean isPanicked()
    {
        return state == PANIC;
    }

    public boolean isCancelled()
    {
        return state == CANCELLED;
    }

    private synchronized void panic( Throwable e )
    {
        upToDateFuture.completeExceptionally( e );
        state = PANIC;
        panicker.panic( new DatabasePanicEvent( databaseContext.databaseId(), CATCHUP_FAILED, e ) );
    }

    private void pullTransactions()
    {
        SocketAddress address;
        try
        {
            address = upstreamProvider.primary( databaseContext.databaseId() );
        }
        catch ( CatchupAddressResolutionException e )
        {
            log.warn( "Could not find upstream database from which to pull. [Message: %s].", e.getMessage() );
            return;
        }

        StoreId localStoreId = databaseContext.storeId();

        pullAndApplyTransactions( address, localStoreId );
    }

    private void streamComplete()
    {
        if ( state == PANIC )
        {
            return;
        }
        BinaryLatch latch = new BinaryLatch();
        applier.applyBatchAsync( new AsyncTaskEventHandler()
        {
            @Override
            public void onFailure( Exception e )
            {
                panic( e );
                latch.release();
            }

            @Override
            public void onSuccess()
            {
                latch.release();
            }
        } );
        latch.await();
    }

    private void pullAndApplyTransactions( SocketAddress address, StoreId localStoreId )
    {
        var lastQueuedTxId = applier.lastQueuedTxId();
        pullRequestMonitor.txPullRequest( lastQueuedTxId );
        log.debug( "Pull transactions from %s where tx id > %d", address, lastQueuedTxId );

        var pullUpdatesJob = new PullUpdatesJob( maxQueueSize, applyBatchSize, new PanicOnApply(), applier, log, () -> state == CANCELLED || state == PANIC );

        TxStreamFinishedResponse result;
        try
        {
            result = catchUpClient
                    .getClient( address, log )
                    .v3( c -> c.pullTransactions( localStoreId, lastQueuedTxId, databaseContext.databaseId() ) )
                    .v4( c -> c.pullTransactions( localStoreId, lastQueuedTxId, databaseContext.databaseId() ) )
                    .v5( c -> c.pullTransactions( localStoreId, lastQueuedTxId, databaseContext.databaseId() ) )
                    .withResponseHandler( pullUpdatesJob )
                    .request();
        }
        catch ( Exception e )
        {
            if ( CancelledPullUpdatesJobException.INSTANCE.equals( e.getCause() ) )
            {
                log.info( "Update job was cancelled. Downloaded transactions will be applied" );
            }
            else
            {
                log.warn( "Exception occurred while pulling transactions. Will retry shortly.", e );
            }
            streamComplete();
            return;
        }

        switch ( result.status() )
        {
        case SUCCESS_END_OF_STREAM:
            log.debug( "Successfully pulled transactions from tx id %d. Completing the up-to-date future %s", lastQueuedTxId, upToDateFuture );
            upToDateFuture.complete( Boolean.TRUE );
            break;
        case E_TRANSACTION_PRUNED:
            log.info( "Tx pull unable to get transactions starting from %d since transactions have been pruned. Attempting a store copy.", lastQueuedTxId );
            transitionToStoreCopy();
            break;
        default:
            log.info( "Tx pull request unable to get transactions > %d ", lastQueuedTxId );
            break;
        }
    }

    private void transitionToStoreCopy()
    {
        if ( state != TX_PULLING )
        {
            return;
        }
        state = STORE_COPYING;
    }

    private void transitionToTxPulling()
    {
        if ( state != STORE_COPYING )
        {
            return;
        }
        state = TX_PULLING;
    }

    private void copyStore()
    {
        try
        {
            ensureKernelStopped();
            storeCopyProcess.replaceWithStoreFrom( upstreamProvider, databaseContext.storeId() );
            if ( restartDatabaseAfterStoreCopy() )
            {
                transitionToTxPulling();
                databaseEventDispatch.fireStoreReplaced( applier.lastQueuedTxId() );
            }
        }
        catch ( IOException | StoreCopyFailedException e )
        {
            log.warn( "Error copying store. Will retry shortly.", e );
        }
        catch ( DatabaseShutdownException e )
        {
            log.warn( "Store copy aborted due to shutdown.", e );
        }
    }

    private void ensureKernelStopped()
    {
        if ( storeCopyHandle == null )
        {
            log.info( "Stopping kernel for store copy" );
            // keep the store copy handle between retries to make sure database doesn't transition to a different state between ticks
            storeCopyHandle = databaseContext.stopForStoreCopy();
        }
        else
        {
            // database is already stopped for store copy by a previous (failed) store-copy attempt
            log.info( "Kernel still stopped for store copy" );
        }
    }

    private boolean restartDatabaseAfterStoreCopy()
    {
        checkState( storeCopyHandle != null, "Store copy handle not initialized" );

        log.info( "Attempting kernel start after store copy" );
        var handle = storeCopyHandle;
        storeCopyHandle = null;
        boolean triggeredReconciler = handle.release();

        if ( !triggeredReconciler )
        {
            log.warn( "Reconciler could not be triggered at this time." );
            return false;
        }
        else if ( !databaseContext.kernelDatabase().isStarted() )
        {
            log.warn( "Kernel did not start properly after the store copy. This might be because of unexpected errors or normal early-exit paths." );
            return false;
        }

        log.info( "Kernel started after store copy" );
        applier = applierFactory.create();
        return true;
    }

    private class PanicOnApply implements AsyncTaskEventHandler
    {
        @Override
        public void onFailure( Exception e )
        {
            panic( e );
        }

        @Override
        public void onSuccess()
        {
            // no-op
        }
    }
}
