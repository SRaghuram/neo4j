/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import org.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public abstract class BaseMarshalTest<T>
{
    private final T original;
    private final ChannelMarshal<T> marshal;

    public BaseMarshalTest( T original, ChannelMarshal<T> marshal )
    {
        this.original = original;
        this.marshal = marshal;
    }

    @Test
    public void shouldMarshalAndUnMarshal() throws IOException, EndOfStreamException
    {
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // when
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( original, writableChannel );

        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        T result = marshal.unmarshal( readableChannel );

        // then
        assertNotSame( original, result );
        assertEquals( original, result );
    }
}
