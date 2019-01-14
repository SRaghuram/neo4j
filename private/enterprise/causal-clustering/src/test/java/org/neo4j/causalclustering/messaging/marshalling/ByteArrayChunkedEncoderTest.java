/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.helpers.Buffers;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ByteArrayChunkedEncoderTest
{
    @Rule
    public final Buffers buffers = new Buffers();

    @Test
    public void shouldWriteToBufferInChunks()
    {
        int chunkSize = 5;
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6};
        byte[] readData = new byte[6];
        ByteArrayChunkedEncoder byteArraySerializer = new ByteArrayChunkedEncoder( data, chunkSize );

        ByteBuf buffer = byteArraySerializer.readChunk( buffers );
        buffer.readBytes( readData, 0, chunkSize );
        assertEquals( 0, buffer.readableBytes() );

        buffer = byteArraySerializer.readChunk( buffers );
        buffer.readBytes( readData, chunkSize, 1 );
        assertArrayEquals( data, readData );
        assertEquals( 0, buffer.readableBytes() );

        assertNull( byteArraySerializer.readChunk( buffers ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowOnTooSmallChunk()
    {
        new ByteArrayChunkedEncoder( new byte[1], 0 );
    }
}
