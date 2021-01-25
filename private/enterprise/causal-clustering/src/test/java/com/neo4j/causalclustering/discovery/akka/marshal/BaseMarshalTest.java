/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
abstract class BaseMarshalTest<T>
{
    abstract Collection<T> originals();
    abstract ChannelMarshal<T> marshal();

    Collection<Object[]> data()
    {
        var marshal = marshal();
        return originals().stream().map( e -> new Object[]{e, marshal} ).collect( Collectors.toList() );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldMarshalAndUnMarshal( T original, ChannelMarshal<T> marshal ) throws IOException, EndOfStreamException
    {
        // given
        var outputStream = new ByteArrayOutputStream();

        // when
        var writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( original, writableChannel );

        var readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        var result = marshal.unmarshal( readableChannel );

        // then
        assertNotSame( original, result );
        assertEquals( original, result );
    }
}
