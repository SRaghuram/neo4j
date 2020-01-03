/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.fs.WritableChannel;

public class ChunkedReplicatedContent implements ChunkedInput<ByteBuf>
{
    private static final int METADATA_SIZE = 1;

    static ChunkedInput<ByteBuf> single( byte contentType, ThrowingConsumer<WritableChannel,IOException> marshaller )
    {
        return chunked( contentType, new Single( marshaller ) );
    }

    static ChunkedInput<ByteBuf> chunked( byte contentType, ChunkedInput<ByteBuf> chunkedInput )
    {
        return new ChunkedReplicatedContent( contentType, chunkedInput );
    }

    private static int metadataSize( boolean isFirstChunk )
    {
        return METADATA_SIZE + (isFirstChunk ? 1 : 0);
    }

    private static ByteBuf writeMetadata( boolean isFirstChunk, boolean isLastChunk, byte contentType, ByteBuf buffer )
    {
        buffer.writeBoolean( isLastChunk );
        if ( isFirstChunk )
        {
            buffer.writeByte( contentType );
        }
        return buffer;
    }

    private final byte contentType;
    private final ChunkedInput<ByteBuf> byteBufAwareMarshal;
    private boolean endOfInput;
    private int progress;

    private ChunkedReplicatedContent( byte contentType, ChunkedInput<ByteBuf> byteBufAwareMarshal )
    {
        this.byteBufAwareMarshal = byteBufAwareMarshal;
        this.contentType = contentType;
    }

    @Override
    public boolean isEndOfInput()
    {
        return endOfInput;
    }

    @Override
    public void close()
    {
        // do nothing
    }

    @Override
    public ByteBuf readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public ByteBuf readChunk( ByteBufAllocator allocator ) throws Exception
    {
        if ( endOfInput )
        {
            return null;
        }
        ByteBuf data = byteBufAwareMarshal.readChunk( allocator );
        if ( data == null )
        {
            return null;
        }
        endOfInput = byteBufAwareMarshal.isEndOfInput();
        CompositeByteBuf allData = new CompositeByteBuf( allocator, false, 2 );
        allData.addComponent( true, data );
        try
        {
            boolean isFirstChunk = progress() == 0;
            int metaDataCapacity = metadataSize( isFirstChunk );
            ByteBuf metaDataBuffer = allocator.buffer( metaDataCapacity, metaDataCapacity );
            allData.addComponent( true, 0, writeMetadata( isFirstChunk, byteBufAwareMarshal.isEndOfInput(), contentType, metaDataBuffer ) );
            progress += allData.readableBytes();
            assert progress > 0; // logic relies on this
            return allData;
        }
        catch ( Throwable e )
        {
            allData.release();
            throw e;
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
        return progress;
    }

    private static class Single implements ChunkedInput<ByteBuf>
    {
        private final ThrowingConsumer<WritableChannel,IOException> marshaller;
        boolean isEndOfInput;
        int offset;

        private Single( ThrowingConsumer<WritableChannel,IOException> marshaller )
        {
            this.marshaller = marshaller;
        }

        @Override
        public boolean isEndOfInput()
        {
            return isEndOfInput;
        }

        @Override
        public void close()
        {
            isEndOfInput = true;
        }

        @Override
        public ByteBuf readChunk( ChannelHandlerContext ctx ) throws Exception
        {
            return readChunk( ctx.alloc() );
        }

        @Override
        public ByteBuf readChunk( ByteBufAllocator allocator ) throws Exception
        {
            if ( isEndOfInput )
            {
                return null;
            }
            ByteBuf buffer = allocator.buffer();
            marshaller.accept( new BoundedNetworkWritableChannel( buffer ) );
            isEndOfInput = true;
            offset = buffer.readableBytes();
            return buffer;
        }

        @Override
        public long length()
        {
            return -1;
        }

        @Override
        public long progress()
        {
            return offset;
        }
    }
}
