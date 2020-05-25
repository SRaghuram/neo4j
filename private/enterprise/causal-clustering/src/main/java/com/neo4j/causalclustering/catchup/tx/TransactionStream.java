/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static java.lang.String.format;

/**
 * Returns a chunked stream of transactions.
 */
public class TransactionStream implements ChunkedInput<Object>
{
    private final Log log;
    private final StoreId storeId;
    private final IOCursor<CommittedTransactionRepresentation> txCursor;
    private final CatchupServerProtocol protocol;
    private final long txIdPromise;

    private boolean endOfInput;
    private boolean noMoreTransactions;
    private long expectedTxId;
    private long lastTxId;

    private final Queue<Object> pending = new LinkedList<>();

    TransactionStream( Log log, TxPullingContext txPullingContext, CatchupServerProtocol protocol )
    {
        this.log = log;
        this.storeId = txPullingContext.localStoreId();
        this.expectedTxId = txPullingContext.firstTxId();
        this.txIdPromise = txPullingContext.txIdPromise();
        this.txCursor = txPullingContext.transactions();
        this.protocol = protocol;
    }

    @Override
    public boolean isEndOfInput()
    {
        return endOfInput;
    }

    @Override
    public void close() throws Exception
    {
        txCursor.close();
    }

    @Override
    public Object readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public Object readChunk( ByteBufAllocator allocator ) throws Exception
    {
        assert !endOfInput;

        if ( !pending.isEmpty() )
        {
            return sendPending();
        }

        if ( !addNextTx() )
        {
            addProperClose();
        }
        return sendPending();
    }

    /**
     * If an exception is present on the queue it has to be throw, signalizing the error to pipeline and/or listeners
     */
    private Object sendPending() throws Exception
    {
        var prevPending = pending.poll();
        if ( pending.isEmpty() && noMoreTransactions )
        {
            endOfInput = true;
        }
        if ( prevPending instanceof Exception )
        {
            throw (Exception) prevPending;
        }
        return prevPending;
    }

    /**
     * Checks and adds next transaction to the queue. If an exception is thrown its error handling adds element to the queue.
     * @return false if nothing was added to the queue (no transaction / no error) so the stream has to be closed properly
     */
    private boolean addNextTx()
    {
        CommittedTransactionRepresentation tx = null;
        try
        {
            if ( txCursor.next() )
            {
                tx = txCursor.get();
            }
            else
            {
                return false;
            }
        }
        catch ( Exception e )
        {
            addErrorClose( e );
            return true;
        }
        addTx( tx );
        return true;
    }

    /**
     * Chunking protocol only requires the {@link ResponseMessageType#TX} to be sent before the first chunk while older protocols requires it to be sent before
     * each transaction. It is also checked here, that only consecutive transactions are sent.
     */
    private void addTx( CommittedTransactionRepresentation tx )
    {
        var isFirst = lastTxId == 0;

        lastTxId = tx.getCommitEntry().getTxId();
        if ( lastTxId != expectedTxId )
        {
            String msg = format( "Transaction cursor out of order. Expected %d but was %d", expectedTxId, lastTxId );
            addErrorClose( new IllegalStateException( msg ) );
        }
        else
        {
            expectedTxId++;
            if ( isFirst )
            {
                pending.add( ResponseMessageType.TX );
            }
            pending.add( new TxPullResponse( storeId, tx ) );
        }
    }

    /**
     * Finish stream with proper result after 0 or more transactions
     */
    private void addProperClose()
    {
        if ( lastTxId != 0 )
        {
            // only send if at least one tx was sent. This lets the encoder know that all txs have been sent.
            pending.add( TxPullResponse.EMPTY );
        }

        CatchupResult result;
        if ( lastTxId >= txIdPromise )
        {
            result = SUCCESS_END_OF_STREAM;
        }
        else
        {
            result = E_TRANSACTION_PRUNED;
            log.warn( "Transaction cursor fell short. Expected at least %d but only got to %d.", txIdPromise, lastTxId );
        }
        addClose( result );
    }

    /**
     * Finish stream with {@link CatchupResult#E_GENERAL_ERROR} result, also adding the exception to the queue. Once reading to queue reaches the exception it
     * will be thrown to signal the error to pipeline and/or listeners
     */
    private void addErrorClose( Exception e )
    {
        addClose( CatchupResult.E_GENERAL_ERROR );
        pending.add( e );
    }

    /**
     * Finish stream with result
     */
    private void addClose( CatchupResult result )
    {
        noMoreTransactions = true;
        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        pending.add( ResponseMessageType.TX_STREAM_FINISHED );
        pending.add( new TxStreamFinishedResponse( result, lastTxId ) );
    }

    @Override
    public long length()
    {
        return -1;
    }

    @Override
    public long progress()
    {
        return 0;
    }

    public long lastTxId()
    {
        return lastTxId;
    }
}
