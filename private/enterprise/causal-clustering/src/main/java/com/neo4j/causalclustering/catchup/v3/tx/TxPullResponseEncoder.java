/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.WritableTxPullResponse;
import com.neo4j.causalclustering.helper.ErrorHandler;
import com.neo4j.causalclustering.messaging.ChunkingNetworkChannel;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.util.LinkedList;

import org.neo4j.io.ByteUnit;
import org.neo4j.util.VisibleForTesting;

public class TxPullResponseEncoder extends ChannelOutboundHandlerAdapter
{
    // careful not to exceed Nettys pool size.
    private static final int DEFAULT_CHUNK_SIZE = (int) ByteUnit.mebiBytes( 1 );
    private static final int PLACEHOLDER_TX_INFO = -1;
    private final LinkedList<ByteBuf> pendingChunks = new LinkedList<>();
    private final int chunkSize;
    private ChunkingNetworkChannel channel;

    @VisibleForTesting
    TxPullResponseEncoder( int chunkSize )
    {
        this.chunkSize = chunkSize;
    }

    public TxPullResponseEncoder()
    {
        this( DEFAULT_CHUNK_SIZE );
    }

    @Override
    public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise ) throws Exception
    {
        if ( msg instanceof WritableTxPullResponse )
        {
            try
            {
                writeResponse( ctx, (WritableTxPullResponse) msg, promise );
            }
            catch ( Exception e )
            {
                // if response was written successfully, there should be no pending chunks in the queue.
                // If there are, this ensures that the buffers are released before rethrowing.
                if ( !pendingChunks.isEmpty() )
                {
                    try ( var errorHandler = new ErrorHandler( "Release buffers" ) )
                    {
                        errorHandler.add( e );
                        pendingChunks.forEach( byteBuf -> errorHandler.execute( () -> ReferenceCountUtil.release( byteBuf ) ) );
                        pendingChunks.clear();
                    }
                }
                else
                {
                    throw e;
                }
            }
        }
        else
        {
            super.write( ctx, msg, promise );
        }
    }

    /**
     * Writes a {@link TxPullResponse} to the pending buffer queue and then flushes any pending chunks to network.
     */
    private void writeResponse( ChannelHandlerContext ctx, WritableTxPullResponse msg, ChannelPromise promise ) throws IOException
    {
        var txPullResponse = msg.txPullResponse();
        var logEntryWriterFactory = msg.logEntryWriterFactory();
        assert pendingChunks.isEmpty();
        if ( isFirstTx() )
        {
            handleFirstTx( ctx, txPullResponse );
        }
        var metadataIndex = channel.currentIndex();
        channel.putInt( PLACEHOLDER_TX_INFO );
        if ( isEndOfTxStream( txPullResponse ) )
        {
            channel.close();
            channel = null;
        }
        else
        {
            logEntryWriterFactory.createEntryWriter( channel ).serialize( txPullResponse.tx() );
        }
        // the size is only required if the tx stretches over multiple chunks
        if ( !pendingChunks.isEmpty() )
        {
            replacePlaceholderSize( metadataIndex );
        }
        // The current buffer must be flushed if there is no room for the next metadata integer
        if ( channel != null && channel.currentIndex() + Integer.BYTES > chunkSize )
        {
            channel.flush();
        }
        writePendingChunks( ctx, promise );
    }

    private void writePendingChunks( ChannelHandlerContext ctx, ChannelPromise promise )
    {
        if ( pendingChunks.isEmpty() )
        {
            promise.setSuccess();
        }
        else
        {
            do
            {
                final var nextChunk = pendingChunks.poll();
                // pass in promise on last write
                var nextPromise = pendingChunks.isEmpty() ? promise : ctx.voidPromise();
                ctx.write( nextChunk, nextPromise );
            }
            while ( !pendingChunks.isEmpty() );
        }
    }

    private void handleFirstTx( ChannelHandlerContext ctx, TxPullResponse msg ) throws IOException
    {
        if ( isEndOfTxStream( msg ) )
        {
            throw new IllegalArgumentException( "Requires at least one transaction." );
        }
        this.channel = new ChunkingNetworkChannel( ctx.alloc(), chunkSize, pendingChunks );

        // meta data for initial chunk
        StoreIdMarshal.INSTANCE.marshal( msg.storeId(), channel );
    }

    /**
     * Replace placeholder size with the serialized size of the tx. The correct tx size is only required if one of the following i true:
     * <p>
     * 1. When transactions span over multiple chunks - This is so that the decoder does not try to serialize transaction before all bytes are present.
     * 2. When closing the tx stream after last tx has been written and the channel has been flushed a final int of size 0 will be written to the buffer -
     * This is so that the decoder will know when the tx stream is completed.
     * <p>
     * Any transaction that fits within a chunk will keep the {@link #PLACEHOLDER_TX_INFO} since it is not a requirement for de-serializing the tx.
     *
     * @param metadataIndex index position of the size integer written before tx serialization.
     */
    private void replacePlaceholderSize( int metadataIndex )
    {
        pendingChunks.peek().setInt( metadataIndex, calculateTxSize( metadataIndex + Integer.BYTES ) );
    }

    /**
     * @param txStartIndex offset
     * @return Total size of the serialized transaction.
     */
    private int calculateTxSize( int txStartIndex )
    {
        // need to subtract both start index an its allocation space.
        return (channel != null ? channel.currentIndex() : 0) + (pendingChunks.stream().mapToInt( ByteBuf::readableBytes ).sum()) - txStartIndex;
    }

    private boolean isEndOfTxStream( TxPullResponse msg )
    {
        return msg.equals( TxPullResponse.EMPTY );
    }

    private boolean isFirstTx()
    {
        return channel == null;
    }
}
