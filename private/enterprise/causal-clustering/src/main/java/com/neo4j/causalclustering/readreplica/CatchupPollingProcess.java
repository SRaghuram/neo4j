/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.tx.PullRequestMonitor;
import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.common.DatabaseService;
import com.neo4j.causalclustering.common.LocalDatabase;
import com.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.helper.Suspendable;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

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

    private final String databaseName;
    private final LocalDatabase localDatabase;
    //TODO: It makes no sense to take both localDatabase and databaseService here. When localDatabase objects can stop and start it won't be needed
    private final DatabaseService databaseService;
    private final Log log;
    private final Suspendable enableDisableOnStoreCopy;
    private final StoreCopyProcess storeCopyProcess;
    private final CatchupClientFactory catchUpClient;
    private final Panicker panicker;
    private final UpstreamDatabaseStrategySelector selectionStrategy;
    private final BatchingTxApplier applier;
    private final PullRequestMonitor pullRequestMonitor;
    private final TopologyService topologyService;
    private final Executor executor;

    private volatile State state = TX_PULLING;
    private CompletableFuture<Boolean> upToDateFuture; // we are up-to-date when we are successfully pulling
    private volatile long latestTxIdOfUpStream;

    public CatchupPollingProcess( Executor executor, String databaseName, DatabaseService databaseService, Suspendable enableDisableOnSoreCopy,
            CatchupClientFactory catchUpClient, UpstreamDatabaseStrategySelector selectionStrategy, BatchingTxApplier applier, Monitors monitors,
            StoreCopyProcess storeCopyProcess, TopologyService topologyService, LogProvider logProvider,  Panicker panicker )

    {
        this.databaseName = databaseName;
        this.databaseService = databaseService;
        this.localDatabase = databaseService.get( databaseName ).orElseThrow( IllegalStateException::new );
        this.enableDisableOnStoreCopy = enableDisableOnSoreCopy;
        this.catchUpClient = catchUpClient;
        this.selectionStrategy = selectionStrategy;
        this.applier = applier;
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
        this.storeCopyProcess = storeCopyProcess;
        this.topologyService = topologyService;
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

    private synchronized void panic( Throwable e )
    {
        upToDateFuture.completeExceptionally( e );
        state = PANIC;
        panicker.panic( e );
    }

    private void pullTransactions()
    {
        MemberId upstream;
        try
        {
            upstream = selectionStrategy.bestUpstreamDatabase();
        }
        catch ( UpstreamDatabaseSelectionException e )
        {
            log.warn( "Could not find upstream database from which to pull.", e );
            return;
        }

        StoreId localStoreId = localDatabase.storeId();

        boolean moreToPull = true;
        int batchCount = 1;
        while ( moreToPull )
        {
            moreToPull = pullAndApplyBatchOfTransactions( upstream, batchCount, localStoreId );
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

    private boolean pullAndApplyBatchOfTransactions( MemberId upstream, int batchCount, StoreId localStoreId )
    {
        long lastQueuedTxId = applier.lastQueuedTxId();
        pullRequestMonitor.txPullRequest( lastQueuedTxId );
        log.debug( "Pull transactions from %s where tx id > %d [batch #%d]", upstream, lastQueuedTxId, batchCount );

        CatchupResponseAdaptor<TxStreamFinishedResponse> responseHandler = new CatchupResponseAdaptor<TxStreamFinishedResponse>()
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
            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( upstream );
            result = catchUpClient.getClient( fromAddress )
                    .v1( c -> c.pullTransactions( localStoreId, lastQueuedTxId ) )
                    .v2( c -> c.pullTransactions( localStoreId, lastQueuedTxId, databaseName ) )
                    .withResponseHandler( responseHandler )
                    .request( log );
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
        try
        {
            databaseService.stopForStoreCopy();
            enableDisableOnStoreCopy.disable();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }

        state = STORE_COPYING;
    }

    private void transitionToTxPulling()
    {
        try
        {
            databaseService.start();
            enableDisableOnStoreCopy.enable();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }

        latestTxIdOfUpStream = 0; // we will find out on the next pull request response
        applier.refreshFromNewStore();

        state = TX_PULLING;
    }

    private void copyStore()
    {
        try
        {
            MemberId source = selectionStrategy.bestUpstreamDatabase();
            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( source );
            storeCopyProcess.replaceWithStoreFrom( new SingleAddressProvider( fromAddress ), localDatabase.storeId() );
            transitionToTxPulling();
        }
        catch ( IOException | StoreCopyFailedException | UpstreamDatabaseSelectionException | TopologyLookupException e )
        {
            log.warn( "Error copying store. Will retry shortly.", e );
        }
        catch ( DatabaseShutdownException e )
        {
            log.warn( "Store copy aborted due to shutdown.", e );
        }
    }

    public String describeState()
    {
        if ( state == TX_PULLING && applier.lastQueuedTxId() > 0 && latestTxIdOfUpStream > 0 )
        {
            return format( "%s is %s (%d of %d)", databaseName, TX_PULLING.name(), applier.lastQueuedTxId(), latestTxIdOfUpStream );
        }
        else
        {
            return format( "%s is %s", databaseName, state.name() );
        }
    }
}
