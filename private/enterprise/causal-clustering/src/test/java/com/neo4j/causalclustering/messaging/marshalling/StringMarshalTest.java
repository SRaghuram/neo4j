/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.test_helpers.Buffers;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

@Buffers.Extension
class StringMarshalTest implements BaseMarshalTest<String>
{
    @Inject
    private Buffers buffers;

    @Override
    public Collection<String> originals()
    {
        return List.of( "ABC123_?", "", "08934208", "abczsd" );
    }

    @Override
    public ChannelMarshal<String> marshal()
    {
        return new SafeChannelMarshal<>()
        {
            @Override
            protected String unmarshal0( ReadableChannel channel ) throws IOException
            {
                return StringMarshal.unmarshal( channel );
            }

            @Override
            public void marshal( String s, WritableChannel channel ) throws IOException
            {
                StringMarshal.marshal( channel, s );
            }
        };
    }

    private String marshalAndUnmarshalUsingBuffers( final String original, ByteBuf buffer )
    {
        StringMarshal.marshal( buffer, original );
        return StringMarshal.unmarshal( buffer );
    }

    @ParameterizedTest
    @MethodSource( "originals" )
    void shouldMarshalAndUnmarshalUsingBuffers( String original )
    {
        // given
        var buffer = buffers.buffer();

        // when
        var result = marshalAndUnmarshalUsingBuffers( original, buffer );

        // then
        assertEquals( original, result );
        assertNotSame( original, result );
    }

    @Test
    void shouldMarshalAndUnmarshalNullUsingBuffer()
    {
        // given
        var buffer = buffers.buffer();

        // when
        var result = marshalAndUnmarshalUsingBuffers( null, buffer );

        // then
        assertNull( result );
    }

    @Test
    void shouldMarshalAndUnmarshalNullUsingChannel() throws IOException, EndOfStreamException
    {
        // given/when
        var result = marshalAndUnmarshal( null, marshal() );
        // then
        assertNull( null );
    }
}
