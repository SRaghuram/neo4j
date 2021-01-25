/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.tx.PullRequestMonitor;
import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.error_handling.DatabasePanicEvent;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.dbms.ClusterInternalDbmsOperator.StoreCopyHandle;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.util.VisibleForTesting;

import static com.neo4j.causalclustering.error_handling.DatabasePanicReason.CatchupFailed;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.CANCELLED;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.PANIC;
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
        CANCELLED
    }

    private final ReadReplicaDatabaseContext databaseContext;
    private final CatchupAddressProvider upstreamProvider;
    private final Log log;
    private final StoreCopyProcess storeCopyProcess;
    private final CatchupClientFactory catchUpClient;
    private final Panicker panicker;
    private final BatchingTxApplier applier;
    private final ReplicatedDatabaseEventDispatch databaseEventDispatch;
    private final PullRequestMonitor pullRequestMonitor;
    private final Executor executor;

    private volatile State state = TX_PULLING;
    private CompletableFuture<Boolean> upToDateFuture; // we are up-to-date when we are successfully pulling
    private StoreCopyHandle storeCopyHandle;

    CatchupPollingProcess( Executor executor, ReadReplicaDatabaseContext databaseContext, CatchupClientFactory catchUpClient, BatchingTxApplier applier,
            ReplicatedDatabaseEventDispatch databaseEventDispatch, StoreCopyProcess storeCopyProcess, LogProvider logProvider, Panicker panicker,
            CatchupAddressProvider upstreamProvider )
    {
        this.databaseContext = databaseContext;
        this.upstreamProvider = upstreamProvider;
        this.catchUpClient = catchUpClient;
        this.applier = applier;
        this.databaseEventDispatch = databaseEventDispatch;
        this.pullRequestMonitor = databaseContext.monitors().newMonitor( PullRequestMonitor.class );
        this.storeCopyProcess = storeCopyProcess;
        this.log = logProvider.getLog( getClass() );
        this.panicker = panicker;
        this.executor = executor;
    }

    @Override
    public synchronized void start()
    {
        log.debug( "Starting catchup polling process %s", this );
        state = TX_PULLING;
        upToDateFuture = new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> upToDateFuture()
    {
        log.debug( "Returning an up-to-date future %s", upToDateFuture );
        return upToDateFuture;
    }

    @Override
    public void stop()
    {
        log.debug( "Stopping catchup polling process %s", this );
        state = CANCELLED;
    }

    @VisibleForTesting
    public State state()
    {
        return state;
    }

    boolean isStoryCopy()
    {
        return state == STORE_COPYING;
    }

    /**
     * Time to catchup!
     * //TODO: Fix error handling further down the stack to bubble up to this level rather than panicking, as that will not complete the future.
     */
    public CompletableFuture<Void> tick()
    {
        if ( state == CANCELLED || state == PANIC )
        {
            return CompletableFuture.completedFuture( null );
        }

        return CompletableFuture.runAsync( () ->
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
                    break;
                default:
                    throw new IllegalStateException( "Tried to execute catchup but was in state " + state );
                }
            }
            catch ( Throwable e )
            {
                throw new CompletionException( e );
            }
        }, executor ).exceptionally( e ->
        {
            panic( e );
            return null;
        } );
    }

    private synchronized void panic( Throwable e )
    {
        upToDateFuture.completeExceptionally( e );
        state = PANIC;
        panicker.panic( new DatabasePanicEvent( databaseContext.databaseId(), CatchupFailed, e ) );
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

    private synchronized void handleTransaction( CommittedTransactionRepresentation tx )
    {
        if ( state == PANIC )
        {
            return;
        }

        try
        {
            applier.queue( tx );
        }
        catch ( Throwable e )
        {
            panic( e );
        }
    }

    private synchronized void streamComplete()
    {
        if ( state == PANIC )
        {
            return;
        }

        try
        {
            applier.applyBatch();
        }
        catch ( Throwable e )
        {
            panic( e );
        }
    }

    private void pullAndApplyTransactions( SocketAddress address, StoreId localStoreId )
    {
        long lastQueuedTxId = applier.lastQueuedTxId();
        pullRequestMonitor.txPullRequest( lastQueuedTxId );
        log.debug( "Pull transactions from %s where tx id > %d", address, lastQueuedTxId );

        CatchupResponseAdaptor<TxStreamFinishedResponse> responseHandler = new CatchupResponseAdaptor<>()
        {
            @Override
            public void onTxPullResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxPullResponse response )
            {
                handleTransaction( response.tx() );
                if ( state == CANCELLED )
                {
                    signal.complete( new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, -1 ) );
                }
            }

            @Override
            public void onTxStreamFinishedResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxStreamFinishedResponse response )
            {
                streamComplete();
                signal.complete( response );
            }

            @Override
            public void onCatchupErrorResponse( CompletableFuture<TxStreamFinishedResponse> signal, CatchupErrorResponse catchupErrorResponse )
            {
                signal.complete( new TxStreamFinishedResponse( catchupErrorResponse.status(), -1 ) );
                log.warn( catchupErrorResponse.message() );
            }
        };

        TxStreamFinishedResponse result;
        try
        {
            result = catchUpClient
                    .getClient( address, log )
                    .v3( c -> c.pullTransactions( localStoreId, lastQueuedTxId, databaseContext.databaseId() ) )
                    .v4( c -> c.pullTransactions( localStoreId, lastQueuedTxId, databaseContext.databaseId() ) )
                    .v5( c -> c.pullTransactions( localStoreId, lastQueuedTxId, databaseContext.databaseId() ) )
                    .withResponseHandler( responseHandler )
                    .request();
        }
        catch ( Exception e )
        {
            log.warn( "Exception occurred while pulling transactions. Will retry shortly.", e );
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
        state = STORE_COPYING;
    }

    private void transitionToTxPulling()
    {
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
        applier.refreshFromNewStore();
        return true;
    }
}
