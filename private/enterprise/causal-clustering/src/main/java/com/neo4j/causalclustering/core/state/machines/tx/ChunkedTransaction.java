/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.helper.ErrorHandler;
import com.neo4j.causalclustering.messaging.ChunkingNetworkChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.ReferenceCountUtil;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.LogEntryWriterFactory;

public class ChunkedTransaction implements ChunkedInput<ByteBuf>
{
    private static final int CHUNK_SIZE = 32 * 1024;
    private final ReplicatedTransactionFactory.TransactionRepresentationWriter txWriter;
    private final DatabaseId databaseId;
    private ChunkingNetworkChannel channel;
    private final Queue<ByteBuf> chunks = new LinkedList<>();

    ChunkedTransaction( TransactionRepresentationReplicatedTransaction tx, LogEntryWriterFactory logEntryWriterFactory )
    {
        this.txWriter = ReplicatedTransactionFactory.transactionalRepresentationWriter( tx.tx(), logEntryWriterFactory );
        this.databaseId = tx.databaseId();
    }

    @Override
    public boolean isEndOfInput()
    {
        return channel != null && channel.closed() && chunks.isEmpty();
    }

    @Override
    public void close()
    {
        try ( ErrorHandler errorHandler = new ErrorHandler( "Closing chunked transaction" ) )
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
            DatabaseIdWithoutNameMarshal.INSTANCE.marshal( databaseId, channel );
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
