/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.helpers.Buffers;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

@Buffers.Extension
class StringMarshalTest
{
    @Inject
    private Buffers buffers;

    @Test
    void shouldSerializeAndDeserializeString()
    {
        // given
        final var TEST_STRING = "ABC123_?";
        final var buffer = buffers.buffer();

        // when
        StringMarshal.marshal( buffer, TEST_STRING );
        var reconstructed = StringMarshal.unmarshal( buffer );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    void shouldSerializeAndDeserializeEmptyString()
    {
        // given
        final var TEST_STRING = "";
        final var buffer = buffers.buffer();

        // when
        StringMarshal.marshal( buffer, TEST_STRING );
        var reconstructed = StringMarshal.unmarshal( buffer );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    void shouldSerializeAndDeserializeNull()
    {
        // given
        final var buffer = buffers.buffer();

        // when
        StringMarshal.marshal( buffer, null );
        var reconstructed = StringMarshal.unmarshal( buffer );

        // then
        assertNull( reconstructed );
    }

    @Test
    void shouldSerializeAndDeserializeStringUsingChannel() throws IOException
    {
        // given
        final var TEST_STRING = "ABC123_?";
        var outputStream = new ByteArrayOutputStream();
        var writableChannel = new OutputStreamWritableChannel( outputStream );

        // when
        StringMarshal.marshal( writableChannel, TEST_STRING );

        var inputStream = new ByteArrayInputStream( outputStream.toByteArray() );
        var readableChannel = new InputStreamReadableChannel( inputStream );
        var reconstructed = StringMarshal.unmarshal( readableChannel );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    void shouldSerializeAndDeserializeEmptyStringUsingChannel() throws IOException
    {
        // given
        final var TEST_STRING = "";
        var outputStream = new ByteArrayOutputStream();
        var writableChannel = new OutputStreamWritableChannel( outputStream );

        // when
        StringMarshal.marshal( writableChannel, TEST_STRING );

        var inputStream = new ByteArrayInputStream( outputStream.toByteArray() );
        var readableChannel = new InputStreamReadableChannel( inputStream );
        var reconstructed = StringMarshal.unmarshal( readableChannel );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    void shouldSerializeAndDeserializeNullUsingChannel() throws IOException
    {
        // given
        var outputStream = new ByteArrayOutputStream();
        var writableChannel = new OutputStreamWritableChannel( outputStream );

        // when
        StringMarshal.marshal( writableChannel, null );

        var inputStream = new ByteArrayInputStream( outputStream.toByteArray() );
        var readableChannel = new InputStreamReadableChannel( inputStream );
        var reconstructed = StringMarshal.unmarshal( readableChannel );

        // then
        assertNull( reconstructed );
    }
}
