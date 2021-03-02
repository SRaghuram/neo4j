/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.test_helpers;

import com.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.stream.Collectors;

import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
public interface BaseMarshalTest<T>
{
    Collection<T> originals();

    ChannelMarshal<T> marshal();

    default boolean unMarshalCreatesNewRefs()
    {
        return true;
    }

    default boolean singletonMarshal()
    {
        return true;
    }

    default Collection<Object[]> arguments()
    {
        var marshal = marshal();
        return originals().stream().map( e -> new Object[]{e, marshal} ).collect( Collectors.toList() );
    }

    @ParameterizedTest
    @MethodSource( "arguments" )
    default void shouldMarshalAndUnMarshalUsingChannels( T original, ChannelMarshal<T> marshal ) throws IOException, EndOfStreamException
    {
        // given/when
        var result = marshalAndUnmarshal( original, marshal );

        // then
        if ( unMarshalCreatesNewRefs() )
        {
            assertNotSame( original, result );
        }
        assertEquals( original, result );
    }

    @Test
    default void marshalShouldBeSingleton()
    {
        if ( singletonMarshal() )
        {
            assertSame( marshal(), marshal() );
        }
    }

    @Test
    default void marshalShouldBeUnconstructable()
    {
        if ( singletonMarshal() )
        {
            var clazz = marshal().getClass();
            var constructors = clazz.getDeclaredConstructors();

            for ( var constructor : constructors )
            {
                constructor.setAccessible( true );
                assertTrue( Modifier.isPrivate( constructor.getModifiers() ),
                            () -> String.format( "All constructors of %s must be private!", clazz.getCanonicalName() ) );
            }
        }
    }

    default T marshalAndUnmarshal( T original, ChannelMarshal<T> marshal ) throws IOException, EndOfStreamException
    {
        // given
        var outputStream = new ByteArrayOutputStream();

        // when
        var writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( original, writableChannel );

        var readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        return marshal.unmarshal( readableChannel );
    }
}
