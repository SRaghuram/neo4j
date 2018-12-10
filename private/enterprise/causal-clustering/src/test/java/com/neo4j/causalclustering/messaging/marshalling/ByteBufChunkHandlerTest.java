/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.helpers.Buffers;
import com.neo4j.causalclustering.messaging.MessageTooBigException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;

import org.neo4j.helpers.collection.Iterators;

public class ByteBufChunkHandlerTest
{
    @Rule
    public final Buffers buffers = new Buffers();

    @Test( expected = MessageTooBigException.class )
    public void shouldThrowExceptioIfToLarge() throws Exception
    {
        MaxTotalSize maxTotalSize = new MaxTotalSize( new PredictableChunkedInput( 10, 1 ), 10 );

        maxTotalSize.readChunk( buffers );
        maxTotalSize.readChunk( buffers );
    }

    @Test
    public void shouldAllowIfNotTooLarge() throws Exception
    {
        MaxTotalSize maxTotalSize = new MaxTotalSize( new PredictableChunkedInput( 10, 1 ), 11 );

        maxTotalSize.readChunk( buffers );
        maxTotalSize.readChunk( buffers );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowIfIllegalSizeValue()
    {
        new MaxTotalSize( new PredictableChunkedInput(), -1 );
    }

    private class PredictableChunkedInput implements ChunkedInput<ByteBuf>
    {
        private final Iterator<Integer> sizes;

        private PredictableChunkedInput( int... sizes )
        {
            this.sizes = Iterators.asIterator( sizes );
        }

        @Override
        public boolean isEndOfInput()
        {
            return !sizes.hasNext();
        }

        @Override
        public void close()
        {

        }

        @Override
        public ByteBuf readChunk( ChannelHandlerContext ctx )
        {
            return readChunk( ctx.alloc() );
        }

        @Override
        public ByteBuf readChunk( ByteBufAllocator allocator )
        {
            Integer size = sizes.next();
            if ( size == null )
            {
                return null;
            }
            return allocator.buffer( size ).writerIndex( size );
        }

        @Override
        public long length()
        {
            return 0;
        }

        @Override
        public long progress()
        {
            return 0;
        }
    }
}
