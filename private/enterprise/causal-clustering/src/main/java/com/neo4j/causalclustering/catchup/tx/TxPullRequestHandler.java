/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.dbms.database.DbmsLogEntryWriterProvider;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;

import static com.neo4j.causalclustering.catchup.CatchupResult.E_INVALID_REQUEST;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_STORE_ID_MISMATCH;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_STORE_UNAVAILABLE;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static java.lang.String.format;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

public class TxPullRequestHandler extends SimpleChannelInboundHandler<TxPullRequest>
{
    private final CatchupServerProtocol protocol;
    private final Database db;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;
    private final LogEntryWriterFactory logEntryWriterFactory;
    private final TxPullRequestsMonitor monitor;
    private final Log log;
    private final TxStreamingStrategy txStreamingStrategy;

    public TxPullRequestHandler( CatchupServerProtocol protocol, Database db, TxStreamingStrategy txStreamingStrategy )
    {
        this.protocol = protocol;
        this.db = db;
        this.transactionIdStore = transactionIdStore( db );
        this.logicalTransactionStore = logicalTransactionStore( db );
        this.logEntryWriterFactory = DbmsLogEntryWriterProvider.resolveEntryWriterFactory( db );
        this.monitor = db.getMonitors().newMonitor( TxPullRequestsMonitor.class );
        this.log = db.getInternalLogProvider().getLog( getClass() );
        this.txStreamingStrategy = txStreamingStrategy;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final TxPullRequest msg ) throws Exception
    {
        monitor.increment();
        var prepare = prepareRequest( msg );

        if ( prepare.isComplete() )
        {
            prepare.complete( ctx );
            protocol.expect( State.MESSAGE_TYPE );
            return;
        }

        var txPullingContext = prepare.txPullingContext();

        var txStream = new TransactionStream( log, txPullingContext, protocol );
        // chunked transaction stream ends the interaction internally and closes the cursor
        ctx.writeAndFlush( txStream ).addListener( f ->
        {
            // if an exception happens during reading from txStream, it has to be handled here, since it will not be propagated up in the netty pipeline
            if ( !f.isSuccess() )
            {
                log.warn( format( "Failed streaming transactions [%d--%d] to %s", txPullingContext.firstTxId(), txStream.lastTxId(),
                        ctx.channel().remoteAddress() ), f.cause() );
                ctx.close();
            }
            else if ( log.isDebugEnabled() )
            {
                log.debug( "Streamed transactions [%d--%d] to %s", txPullingContext.firstTxId(), txStream.lastTxId(), ctx.channel().remoteAddress() );
            }
        } );
    }

    private Prepare prepareRequest( TxPullRequest msg ) throws IOException
    {
        if ( !isValid( msg ) )
        {
            return Prepare.fail( E_INVALID_REQUEST );
        }

        var firstTxId = msg.previousTxId() + 1;

        if ( !databaseIsAvailable() )
        {
            log.info( "Failed to serve TxPullRequest for tx %d because the local database is unavailable.", firstTxId );
            return Prepare.fail( E_STORE_UNAVAILABLE );
        }
        var expectedStoreId = msg.expectedStoreId();
        var localStoreId = db.getStoreId();
        if ( !localStoreId.equals( expectedStoreId ) )
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
        catch ( Exception e )
        {
            // Could be caused by closed transactionIdStore
            log.info( "Failed to serve TxPullRequest. Reason: %s", e.getMessage() );
            return Prepare.fail( E_STORE_UNAVAILABLE );
        }
        if ( txIdPromise < firstTxId )
        {
            return Prepare.nothingToSend( txIdPromise );
        }

        try
        {
            var transactions = logicalTransactionStore.getTransactions( firstTxId );
            var constraint = createConstraint( txIdPromise );
            return Prepare.readyToSend( new TxPullingContext( transactions, localStoreId, db.getNamedDatabaseId(),
                    firstTxId, txIdPromise, transactionIdStore, logEntryWriterFactory, constraint ) );
        }
        catch ( NoSuchTransactionException e )
        {
            log.info( "Failed to serve TxPullRequest for tx %d because the transaction does not exist. Last committed tx %d", firstTxId, txIdPromise );
            return Prepare.fail( E_TRANSACTION_PRUNED );
        }
    }

    private TxStreamingConstraint createConstraint( long lastCommitTxId )
    {
        switch ( txStreamingStrategy )
        {
        case START_TIME:
            return new TxStreamingConstraint.Limited( lastCommitTxId );
        case UP_TO_DATE:
            return new TxStreamingConstraint.Unbounded();
        default:
            throw new IllegalArgumentException( " Unknown strategy " + txStreamingStrategy );
        }
    }

    private boolean isValid( TxPullRequest msg )
    {
        var previousTxId = msg.previousTxId();
        if ( previousTxId < BASE_TX_ID )
        {
            log.error( "Illegal tx pull request. Tx id must be greater or equal to %s but was %s", BASE_TX_ID, previousTxId );
            return false;
        }
        return true;
    }

    private boolean databaseIsAvailable()
    {
        return db.getDatabaseAvailabilityGuard().isAvailable();
    }

    private static TransactionIdStore transactionIdStore( Database db )
    {
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
    }

    private static LogicalTransactionStore logicalTransactionStore( Database db )
    {
        return db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
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
            return new Prepare( null, txPullingContext.txIdPromise(), txPullingContext );
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
