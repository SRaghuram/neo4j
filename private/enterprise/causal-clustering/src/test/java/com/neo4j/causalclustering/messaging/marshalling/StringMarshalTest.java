/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.helpers.Buffers;
import io.netty.buffer.ByteBuf;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class StringMarshalTest
{
    @Rule
    public final Buffers buffers = new Buffers();

    @Test
    public void shouldSerializeAndDeserializeString()
    {
        // given
        final String TEST_STRING = "ABC123_?";
        final ByteBuf buffer = buffers.buffer();

        // when
        StringMarshal.marshal( buffer, TEST_STRING );
        String reconstructed = StringMarshal.unmarshal( buffer );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    public void shouldSerializeAndDeserializeEmptyString()
    {
        // given
        final String TEST_STRING = "";
        final ByteBuf buffer = buffers.buffer();

        // when
        StringMarshal.marshal( buffer, TEST_STRING );
        String reconstructed = StringMarshal.unmarshal( buffer );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    public void shouldSerializeAndDeserializeNull()
    {
        // given
        final ByteBuf buffer = buffers.buffer();

        // when
        StringMarshal.marshal( buffer, null );
        String reconstructed = StringMarshal.unmarshal( buffer );

        // then
        assertNull( reconstructed );
    }

    @Test
    public void shouldSerializeAndDeserializeStringUsingChannel() throws IOException
    {
        // given
        final String TEST_STRING = "ABC123_?";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );

        // when
        StringMarshal.marshal( writableChannel, TEST_STRING );

        ByteArrayInputStream inputStream = new ByteArrayInputStream( outputStream.toByteArray() );
        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( inputStream );
        String reconstructed = StringMarshal.unmarshal( readableChannel );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    public void shouldSerializeAndDeserializeEmptyStringUsingChannel() throws IOException
    {
        // given
        final String TEST_STRING = "";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );

        // when
        StringMarshal.marshal( writableChannel, TEST_STRING );

        ByteArrayInputStream inputStream = new ByteArrayInputStream( outputStream.toByteArray() );
        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( inputStream );
        String reconstructed = StringMarshal.unmarshal( readableChannel );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    public void shouldSerializeAndDeserializeNullUsingChannel() throws IOException
    {
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );

        // when
        StringMarshal.marshal( writableChannel, null );

        ByteArrayInputStream inputStream = new ByteArrayInputStream( outputStream.toByteArray() );
        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( inputStream );
        String reconstructed = StringMarshal.unmarshal( readableChannel );

        // then
        assertNull( reconstructed );
    }
}
