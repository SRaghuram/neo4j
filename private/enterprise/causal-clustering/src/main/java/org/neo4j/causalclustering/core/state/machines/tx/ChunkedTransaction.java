/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.ReferenceCountUtil;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.causalclustering.helper.ErrorHandler;
import org.neo4j.causalclustering.messaging.ChunkingNetworkChannel;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

class ChunkedTransaction implements ChunkedInput<ByteBuf>
{
    private static final int CHUNK_SIZE = 32 * 1024;
    private final ReplicatedTransactionFactory.TransactionRepresentationWriter txWriter;
    private ChunkingNetworkChannel channel;
    private Queue<ByteBuf> chunks = new LinkedList<>();

    ChunkedTransaction( TransactionRepresentation tx )
    {
        txWriter = ReplicatedTransactionFactory.transactionalRepresentationWriter( tx );
    }

    @Override
    public boolean isEndOfInput()
    {
        return channel != null && channel.closed() && chunks.isEmpty();
    }

    @Override
    public void close()
    {
        try ( ErrorHandler errorHandler = new ErrorHandler( "Closing ChunkedTransaction" ) )
        {
            if ( channel != null )
            {
                errorHandler.execute( () -> channel.close() );
            }
            chunks.forEach( byteBuf -> errorHandler.execute( () -> ReferenceCountUtil.release( byteBuf ) ) );
        }
    }

    @Override
    public ByteBuf readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public ByteBuf readChunk( ByteBufAllocator allocator ) throws Exception
    {
        if ( isEndOfInput() )
        {
            return null;
        }
        if ( channel == null )
        {
            // Ensure that the written buffers does not overflow the allocators chunk size.
            channel = new ChunkingNetworkChannel( allocator, CHUNK_SIZE, chunks );
        }

        // write to chunks if empty and there is more to write
        while ( txWriter.canWrite() && chunks.isEmpty() )
        {
            txWriter.write( channel );
        }
        // nothing more to write, close the channel to get the potential last buffer
        if ( chunks.isEmpty() )
        {
            channel.close();
        }
        return chunks.poll();
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
}
