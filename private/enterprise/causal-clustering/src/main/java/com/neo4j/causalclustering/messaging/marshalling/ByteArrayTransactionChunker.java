/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.state.machines.tx.ByteArrayReplicatedTransaction;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

public class ByteArrayTransactionChunker implements ChunkedInput<ByteBuf>
{
    private final String databaseName;
    private final ByteArrayChunkedEncoder byteChunker;

    private boolean dbNameWritten;

    public ByteArrayTransactionChunker( ByteArrayReplicatedTransaction tx )
    {
        this.byteChunker = new ByteArrayChunkedEncoder( tx.getTxBytes() );
        this.databaseName = tx.databaseName();
    }

    @Override
    public boolean isEndOfInput()
    {
        return byteChunker.isEndOfInput();
    }

    @Override
    public void close()
    {
        byteChunker.close();
    }

    @Override
    public ByteBuf readChunk( ChannelHandlerContext ctx )
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public ByteBuf readChunk( ByteBufAllocator allocator )
    {
        if ( isEndOfInput() )
        {
            return null;
        }
        else if ( !dbNameWritten )
        {
            ByteBuf buffer = allocator.buffer();
            try
            {
                StringMarshal.marshal( buffer, databaseName );
                dbNameWritten = true;
                return buffer;
            }
            catch ( Throwable t )
            {
                buffer.release();
                throw t;
            }
        }
        else
        {
            return byteChunker.readChunk( allocator );
        }
    }

    @Override
    public long length()
    {
        return byteChunker.length();
    }

    @Override
    public long progress()
    {
        return byteChunker.progress();
    }
}
