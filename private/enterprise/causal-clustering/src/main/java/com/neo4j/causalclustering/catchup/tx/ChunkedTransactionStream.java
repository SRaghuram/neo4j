/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
public class ChunkedTransactionStream implements ChunkedInput<Object>
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

    private Object pending;

    ChunkedTransactionStream( Log log, StoreId storeId, long firstTxId, long txIdPromise, IOCursor<CommittedTransactionRepresentation> txCursor,
            CatchupServerProtocol protocol )
    {
        this.log = log;
        this.storeId = storeId;
        this.expectedTxId = firstTxId;
        this.txIdPromise = txIdPromise;
        this.txCursor = txCursor;
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

        if ( pending != null )
        {
            if ( noMoreTransactions )
            {
                endOfInput = true;
            }

            return consumePending();
        }
        else if ( noMoreTransactions )
        {
            /* finalization should always have a last ending message */
            throw new IllegalStateException();
        }
        else if ( txCursor.next() )
        {
            assert pending == null;

            CommittedTransactionRepresentation tx = txCursor.get();
            lastTxId = tx.getCommitEntry().getTxId();
            if ( lastTxId != expectedTxId )
            {
                String msg = format( "Transaction cursor out of order. Expected %d but was %d", expectedTxId, lastTxId );
                throw new IllegalStateException( msg );
            }
            expectedTxId++;
            pending = new TxPullResponse( storeId, tx );
            return ResponseMessageType.TX;
        }
        else
        {
            assert pending == null;

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
            pending = new TxStreamFinishedResponse( result, lastTxId );
            return ResponseMessageType.TX_STREAM_FINISHED;
        }
    }

    private Object consumePending()
    {
        Object prevPending = pending;
        pending = null;
        return prevPending;
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
