/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import org.neo4j.causalclustering.messaging.MessageTooBigException;
import org.neo4j.io.ByteUnit;

import static java.lang.String.format;
import static org.neo4j.util.Preconditions.requirePositive;

public class MaxTotalSize implements ChunkedInput<ByteBuf>
{
    private final ChunkedInput<ByteBuf> chunkedInput;
    private final int maxSize;
    private int totalSize;
    private static final int DEFAULT_MAX_SIZE = (int) ByteUnit.gibiBytes( 1 );

    MaxTotalSize( ChunkedInput<ByteBuf> chunkedInput, int maxSize )
    {
        requirePositive( maxSize );
        this.chunkedInput = chunkedInput;
        this.maxSize = maxSize;
    }

    MaxTotalSize( ChunkedInput<ByteBuf> chunkedInput )
    {
        this( chunkedInput, DEFAULT_MAX_SIZE );
    }

    @Override
    public boolean isEndOfInput() throws Exception
    {
        return chunkedInput.isEndOfInput();
    }

    @Override
    public void close() throws Exception
    {
        chunkedInput.close();
    }

    @Override
    public ByteBuf readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public ByteBuf readChunk( ByteBufAllocator allocator ) throws Exception
    {
        ByteBuf byteBuf = chunkedInput.readChunk( allocator );
        if ( byteBuf != null )
        {
            int additionalBytes = byteBuf.readableBytes();
            this.totalSize += additionalBytes;
            if ( this.totalSize > maxSize )
            {
                throw new MessageTooBigException( format( "Size limit exceeded. Limit is %d, wanted to write %d, written so far %d", maxSize, additionalBytes,
                        totalSize - additionalBytes ) );
            }
        }
        return byteBuf;
    }

    @Override
    public long length()
    {
        return chunkedInput.length();
    }

    @Override
    public long progress()
    {
        return chunkedInput.progress();
    }
}
