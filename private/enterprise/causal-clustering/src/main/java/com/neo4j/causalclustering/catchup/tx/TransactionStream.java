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
            Object prevPending = pending.poll();
            if ( pending.isEmpty() && noMoreTransactions )
            {
                endOfInput = true;
            }
            return prevPending;
        }
        else if ( txCursor.next() )
        {
            boolean isFirst = lastTxId == 0;

            CommittedTransactionRepresentation tx = txCursor.get();
            lastTxId = tx.getCommitEntry().getTxId();
            if ( lastTxId != expectedTxId )
            {
                String msg = format( "Transaction cursor out of order. Expected %d but was %d", expectedTxId, lastTxId );
                throw new IllegalStateException( msg );
            }
            expectedTxId++;
            return sendTx( isFirst, tx );
        }
        else
        {
            if ( lastTxId != 0 )
            {
                // only send if at least one tx was sent. This lets the encoder know that all txs have been sent.
                pending.add( TxPullResponse.EMPTY );
            }
            noMoreTransactions = true;
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
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
            pending.add( ResponseMessageType.TX_STREAM_FINISHED );
            pending.add( new TxStreamFinishedResponse( result, lastTxId ) );
            return pending.poll();
        }
    }

    /**
     * Chunking protocol only requires the {@link ResponseMessageType#TX} to be sent before the first chunk while
     * older protocols requires it to be sent before each transaction.
     */
    private Object sendTx( boolean isFirst, CommittedTransactionRepresentation tx )
    {
        if ( isFirst )
        {
            pending.add( new TxPullResponse( storeId, tx ) );
            return ResponseMessageType.TX;
        }
        else
        {
            return new TxPullResponse( storeId, tx );
        }
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
