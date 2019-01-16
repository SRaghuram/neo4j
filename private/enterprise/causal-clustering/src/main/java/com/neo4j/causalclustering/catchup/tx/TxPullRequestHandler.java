/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import com.neo4j.causalclustering.identity.StoreId;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.store.StoreFileClosedException;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.catchup.CatchupResult.E_INVALID_REQUEST;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_STORE_ID_MISMATCH;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_STORE_UNAVAILABLE;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static java.lang.String.format;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class TxPullRequestHandler extends SimpleChannelInboundHandler<TxPullRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<StoreId> storeIdSupplier;
    private final BooleanSupplier databaseAvailable;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;
    private final TxPullRequestsMonitor monitor;
    private final Log log;

    public TxPullRequestHandler( CatchupServerProtocol protocol, Supplier<StoreId> storeIdSupplier,
            BooleanSupplier databaseAvailable, Supplier<Database> dataSourceSupplier, Monitors monitors, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.storeIdSupplier = storeIdSupplier;
        this.databaseAvailable = databaseAvailable;
        DependencyResolver dependencies = dataSourceSupplier.get().getDependencyResolver();
        this.transactionIdStore = dependencies.resolveDependency( TransactionIdStore.class );
        this.logicalTransactionStore = dependencies.resolveDependency( LogicalTransactionStore.class );
        this.monitor = monitors.newMonitor( TxPullRequestsMonitor.class );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final TxPullRequest msg ) throws Exception
    {
        monitor.increment();
        Prepare prepare = prepareRequest( msg );

        if ( prepare.isComplete() )
        {
            prepare.complete( ctx );
            protocol.expect( State.MESSAGE_TYPE );
            return;
        }

        TxPullingContext txPullingContext = prepare.txPullingContext();

        ChunkedTransactionStream txStream =
                new ChunkedTransactionStream( log, txPullingContext.localStoreId, txPullingContext.firstTxId, txPullingContext.txIdPromise,
                        txPullingContext.transactions, protocol );
        // chunked transaction stream ends the interaction internally and closes the cursor
        ctx.writeAndFlush( txStream ).addListener( f ->
        {
            if ( log.isDebugEnabled() || !f.isSuccess() )
            {
                String message =
                        format( "Streamed transactions [%d--%d] to %s", txPullingContext.firstTxId, txStream.lastTxId(), ctx.channel().remoteAddress() );
                if ( f.isSuccess() )
                {
                    log.debug( message );
                }
                else
                {
                    log.warn( message, f.cause() );
                }
            }
        } );

    }

    private Prepare prepareRequest( TxPullRequest msg ) throws IOException
    {
        if ( !isValid( msg ) )
        {
            return Prepare.fail( E_INVALID_REQUEST );
        }

        long firstTxId = msg.previousTxId() + 1;

        if ( !databaseAvailable.getAsBoolean() )
        {
            log.info( "Failed to serve TxPullRequest for tx %d because the local database is unavailable.", firstTxId );
            return Prepare.fail( E_STORE_UNAVAILABLE );
        }
        StoreId expectedStoreId = msg.expectedStoreId();
        StoreId localStoreId = storeIdSupplier.get();
        if ( localStoreId == null || !localStoreId.equals( expectedStoreId ) )
        {
            log.info( "Failed to serve TxPullRequest for tx %d and storeId %s because that storeId is different from this machine with %s", firstTxId,
                    expectedStoreId, localStoreId );
            return Prepare.fail( E_STORE_ID_MISMATCH );
        }

        /*
         * This is the minimum transaction id we must send to consider our streaming operation successful. The kernel can
         * concurrently prune even future transactions while iterating and the cursor will silently fail on iteration, so
         * we need to add our own protection for this reason and also as a generally important sanity check for the fulfillment
         * of the consistent recovery contract which requires us to stream transactions at least as far as the time when the
         * file copy operation completed.
         */
        long txIdPromise;
        try
        {
            txIdPromise = transactionIdStore.getLastCommittedTransactionId();
        }
        catch ( StoreFileClosedException e )
        {
            log.info( "Failed to serve TxPullRequest. Reason: %s", e.getMessage() );
            return Prepare.fail( E_STORE_UNAVAILABLE );
        }
        if ( txIdPromise < firstTxId )
        {
            return Prepare.nothingToSend( txIdPromise );
        }

        try
        {
            TransactionCursor transactions = logicalTransactionStore.getTransactions( firstTxId );
            return Prepare.readyToSend( new TxPullingContext( transactions, localStoreId, firstTxId, txIdPromise ) );
        }
        catch ( NoSuchTransactionException e )
        {
            log.info( "Failed to serve TxPullRequest for tx %d because the transaction does not exist.", firstTxId );
            return Prepare.fail( E_TRANSACTION_PRUNED );
        }
    }

    private boolean isValid( TxPullRequest msg )
    {
        long previousTxId = msg.previousTxId();
        if ( previousTxId < BASE_TX_ID )
        {
            log.error( "Illegal tx pull request. Tx id must be greater or equal to %s but was %s", BASE_TX_ID, previousTxId );
            return false;
        }
        return true;
    }

    private static class TxPullingContext
    {
        private final TransactionCursor transactions;
        private final StoreId localStoreId;
        private final long firstTxId;
        private final long txIdPromise;

        TxPullingContext( TransactionCursor transactions, StoreId localStoreId, long firstTxId, long txIdPromise )
        {
            this.transactions = transactions;
            this.localStoreId = localStoreId;
            this.firstTxId = firstTxId;
            this.txIdPromise = txIdPromise;
        }
    }

    private static class Prepare
    {
        private final CatchupResult catchupResult;
        private final long txId;
        private final TxPullingContext txPullingContext;

        private Prepare( CatchupResult catchupResult, long txId, TxPullingContext txPullingContext )
        {
            this.catchupResult = catchupResult;
            this.txId = txId;
            this.txPullingContext = txPullingContext;
        }

        static Prepare fail( CatchupResult catchupResult )
        {
            return new Prepare( catchupResult, -1, null );
        }

        static Prepare readyToSend( TxPullingContext txPullingContext )
        {
            return new Prepare( null, txPullingContext.txIdPromise, txPullingContext );
        }

        static Prepare nothingToSend( long txIdPromise )
        {
            return new Prepare( SUCCESS_END_OF_STREAM, txIdPromise, null );
        }

        public boolean isComplete()
        {
            return catchupResult != null;
        }

        private void complete( ChannelHandlerContext ctx )
        {
            if ( catchupResult == null )
            {
                throw new IllegalStateException( "Cannot complete catchup request." );
            }
            ctx.write( ResponseMessageType.TX_STREAM_FINISHED );
            ctx.writeAndFlush( new TxStreamFinishedResponse( catchupResult, txId ) );
        }

        TxPullingContext txPullingContext()
        {
            return txPullingContext;
        }
    }
}
