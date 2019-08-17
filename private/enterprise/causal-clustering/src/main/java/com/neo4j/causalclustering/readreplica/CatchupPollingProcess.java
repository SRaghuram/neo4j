/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.tx.PullRequestMonitor;
import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

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

import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.CANCELLED;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.PANIC;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.STORE_COPYING;
import static com.neo4j.causalclustering.readreplica.CatchupPollingProcess.State.TX_PULLING;
import static java.lang.String.format;

/**
 * This class is responsible for pulling transactions from a core server and queuing
 * them to be applied with the {@link BatchingTxApplier}.
 */
public class CatchupPollingProcess extends LifecycleAdapter
{
    enum State
    {
        TX_PULLING,
        STORE_COPYING,
        PANIC,
        CANCELLED
    }

    private final ReadReplicaDatabaseContext databaseContext;
    private final CatchupAddressProvider catchupAddressProvider;
    private final Log log;
    private final StoreCopyProcess storeCopyProcess;
    private final CatchupClientFactory catchUpClient;
    private final DatabasePanicker panicker;
    private final BatchingTxApplier applier;
    private final PullRequestMonitor pullRequestMonitor;
    private final Executor executor;

    private volatile State state = TX_PULLING;
    private CompletableFuture<Boolean> upToDateFuture; // we are up-to-date when we are successfully pulling
    private volatile long latestTxIdOfUpStream;

    CatchupPollingProcess( Executor executor, ReadReplicaDatabaseContext databaseContext,
            CatchupClientFactory catchUpClient, BatchingTxApplier applier, StoreCopyProcess storeCopyProcess,
            LogProvider logProvider, DatabasePanicker panicker, CatchupAddressProvider catchupAddressProvider )

    {
        this.databaseContext = databaseContext;
        this.catchupAddressProvider = catchupAddressProvider;
        this.catchUpClient = catchUpClient;
        this.applier = applier;
        this.pullRequestMonitor = databaseContext.monitors().newMonitor( PullRequestMonitor.class );
        this.storeCopyProcess = storeCopyProcess;
        this.log = logProvider.getLog( getClass() );
        this.panicker = panicker;
        this.executor = executor;
    }

    @Override
    public synchronized void start()
    {
        state = TX_PULLING;
        upToDateFuture = new CompletableFuture<>();
    }

    public CompletableFuture<Boolean> upToDateFuture()
    {
        return upToDateFuture;
    }

    @Override
    public void stop()
    {
        state = CANCELLED;
    }

    public State state()
    {
        return state;
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

    String describeState()
    {
        if ( state == TX_PULLING && applier.lastQueuedTxId() > 0 && latestTxIdOfUpStream > 0 )
        {
            return format( "%s is %s (%d of %d)", databaseContext.databaseId(), TX_PULLING.name(), applier.lastQueuedTxId(), latestTxIdOfUpStream );
        }
        else
        {
            return String.format( "%s is %s", databaseContext.databaseId(), state.name() );
        }
    }

    private synchronized void panic( Throwable e )
    {
        upToDateFuture.completeExceptionally( e );
        state = PANIC;
        panicker.panic( e );
    }

    private void pullTransactions()
    {
        SocketAddress address;
        try
        {
            address = catchupAddressProvider.primary( databaseContext.databaseId() );
        }
        catch ( CatchupAddressResolutionException e )
        {
            log.warn( "Could not find upstream database from which to pull.", e );
            return;
        }

        StoreId localStoreId = databaseContext.storeId();

        boolean moreToPull = true;
        int batchCount = 1;
        while ( moreToPull )
        {
            moreToPull = pullAndApplyBatchOfTransactions( address, batchCount, localStoreId );
            batchCount++;
        }
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

    private boolean pullAndApplyBatchOfTransactions( SocketAddress address, int batchCount, StoreId localStoreId )
    {
        long lastQueuedTxId = applier.lastQueuedTxId();
        pullRequestMonitor.txPullRequest( lastQueuedTxId );
        log.debug( "Pull transactions from %s where tx id > %d [batch #%d]", address, lastQueuedTxId, batchCount );

        CatchupResponseAdaptor<TxStreamFinishedResponse> responseHandler = new CatchupResponseAdaptor<>()
        {
            @Override
            public void onTxPullResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxPullResponse response )
            {
                handleTransaction( response.tx() );
            }

            @Override
            public void onTxStreamFinishedResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxStreamFinishedResponse response )
            {
                streamComplete();
                signal.complete( response );
            }
        };

        TxStreamFinishedResponse result;
        try
        {
            result = catchUpClient
                    .getClient( address, log )
                    .v3( c -> c.pullTransactions( localStoreId, lastQueuedTxId, databaseContext.databaseId() ) )
                    .withResponseHandler( responseHandler )
                    .request();
        }
        catch ( Exception e )
        {
            log.warn( "Exception occurred while pulling transactions. Will retry shortly.", e );
            streamComplete();
            return false;
        }

        latestTxIdOfUpStream = result.lastTxId();

        switch ( result.status() )
        {
        case SUCCESS_END_OF_STREAM:
            log.debug( "Successfully pulled transactions from tx id %d", lastQueuedTxId );
            upToDateFuture.complete( Boolean.TRUE );
            return false;
        case E_TRANSACTION_PRUNED:
            log.info( "Tx pull unable to get transactions starting from %d since transactions have been pruned. Attempting a store copy.", lastQueuedTxId );
            transitionToStoreCopy();
            return false;
        default:
            log.info( "Tx pull request unable to get transactions > %d " + lastQueuedTxId );
            return false;
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
            var stoppedDb = databaseContext.stopForStoreCopy();
            storeCopyProcess.replaceWithStoreFrom( catchupAddressProvider, databaseContext.storeId() );
            transitionToTxPulling();
            restartDatabase( stoppedDb );
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

    private void restartDatabase( ClusterInternalDbmsOperator.StoreCopyHandle stoppedDb )
    {
        try
        {
            stoppedDb.restart();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }

        latestTxIdOfUpStream = 0; // we will find out on the next pull request response
        applier.refreshFromNewStore();
    }
}
