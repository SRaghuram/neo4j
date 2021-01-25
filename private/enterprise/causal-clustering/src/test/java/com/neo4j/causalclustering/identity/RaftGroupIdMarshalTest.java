/*
 * Copyright (c) "Neo4j"
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

class RaftGroupIdMarshalTest
{
    private ChannelMarshal<RaftGroupId> marshal = RaftGroupId.Marshal.INSTANCE;

    @Test
    void shouldMarshalRaftId() throws Throwable
    {
        // given
        RaftGroupId original = IdFactory.randomRaftId();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // when
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( original, writableChannel );

        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        RaftGroupId result = marshal.unmarshal( readableChannel );

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
        RaftGroupId result = marshal.unmarshal( readableChannel );

        // then
        assertNull( result );
    }
}
