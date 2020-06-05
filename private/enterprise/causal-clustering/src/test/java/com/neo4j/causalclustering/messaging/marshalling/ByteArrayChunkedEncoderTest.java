/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.helpers.Buffers;
import org.junit.jupiter.api.Test;

import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Buffers.Extension
public class ByteArrayChunkedEncoderTest
{
    @Inject
    private Buffers buffers;

    @Test
    void shouldWriteToBufferInChunks()
    {
        var chunkSize = 5;
        var data = new byte[]{1, 2, 3, 4, 5, 6};
        var readData = new byte[6];
        var byteArraySerializer = new ByteArrayChunkedEncoder( data, chunkSize );

        var buffer = byteArraySerializer.readChunk( buffers );
        buffer.readBytes( readData, 0, chunkSize );
        assertEquals( 0, buffer.readableBytes() );

        buffer = byteArraySerializer.readChunk( buffers );
        buffer.readBytes( readData, chunkSize, 1 );
        assertArrayEquals( data, readData );
        assertEquals( 0, buffer.readableBytes() );

        assertNull( byteArraySerializer.readChunk( buffers ) );
    }

    @Test
    void shouldThrowOnTooSmallChunk()
    {
        assertThrows( IllegalArgumentException.class, () -> new ByteArrayChunkedEncoder( new byte[1], 0 ) );
    }
}
