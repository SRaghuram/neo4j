/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.helpers.Buffers;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;

import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Buffers.Extension
class ChunkingNetworkChannelTest
{
    @Inject
    private Buffers buffers;

    @Test
    void shouldSerializeIntoChunksOfGivenSize()
    {
        // given
        var chunkSize = 8;
        var byteBufs = new LinkedList<ByteBuf>();
        var channel = new ChunkingNetworkChannel( buffers, chunkSize, byteBufs );

        // and data is written
        var array = new byte[10];
        channel.put( (byte) 1 );
        channel.putInt( 1 );
        channel.putFloat( 1.0f );
        channel.putDouble( 1.0d );
        channel.putShort( (short) 1 );
        channel.putLong( 1 );
        channel.put( array, array.length );
        channel.flush();

        // when
        var combinedByteBuf = buffers.buffer();
        ByteBuf byteBuf;
        while ( (byteBuf = byteBufs.poll()) != null )
        {
            assertEquals( chunkSize, byteBuf.capacity() );
            combinedByteBuf.writeBytes( byteBuf );
        }

        //then
        assertEquals( (byte) 1, combinedByteBuf.readByte() );
        assertEquals( 1, combinedByteBuf.readInt() );
        assertEquals( 1.0f, combinedByteBuf.readFloat() );
        assertEquals( 1.0d, combinedByteBuf.readDouble() );
        assertEquals( (short) 1, combinedByteBuf.readShort() );
        assertEquals( 1L, combinedByteBuf.readLong() );
        byte[] bytes = new byte[array.length];
        combinedByteBuf.readBytes( bytes );
        assertArrayEquals( array, bytes );
        assertEquals( 0, combinedByteBuf.readableBytes() );
    }

    @Test
    void shouldReturnNullIfQueueIsEmpty()
    {
        // given
        var chunkSize = 8;
        var byteBufs = new LinkedList<ByteBuf>();

        var channel = new ChunkingNetworkChannel( buffers, chunkSize, byteBufs );

        // when
        channel.putLong( 1L );
        channel.putLong( 1L );

        // then
        assertNotNull( byteBufs.poll() );
        assertNull( byteBufs.poll() );

        // when
        channel.putLong( 2L );

        // then
        assertNotNull( byteBufs.poll() );
        assertNull( byteBufs.poll() );

        // when
        channel.flush();

        // then
        assertNotNull( byteBufs.poll() );
    }

    @Test
    void shouldThrowIllegalStatAfterClosed()
    {
        int chunkSize = 8;
        var channel = new ChunkingNetworkChannel( buffers, chunkSize, new LinkedList<>() );
        channel.close();
        assertThrows( IllegalStateException.class, () -> channel.putInt( 1 ) );
    }
}
