/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.neo4j.io.marshal.ChannelMarshal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

class RaftIdMarshalTest
{
    private ChannelMarshal<RaftId> marshal = RaftId.Marshal.INSTANCE;

    @Test
    void shouldMarshalRaftId() throws Throwable
    {
        // given
        RaftId original = RaftIdFactory.random();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // when
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( original, writableChannel );

        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        RaftId result = marshal.unmarshal( readableChannel );

        // then
        assertNotSame( original, result );
        assertEquals( original, result );
    }

    @Test
    void shouldMarshalNullRaftId() throws Throwable
    {
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // when
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( null, writableChannel );

        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        RaftId result = marshal.unmarshal( readableChannel );

        // then
        assertNull( result );
    }
}
