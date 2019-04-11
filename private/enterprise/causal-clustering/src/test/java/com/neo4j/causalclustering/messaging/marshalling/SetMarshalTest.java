/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SetMarshalTest
{
    @Test
    void emptySet() throws Exception
    {
        var expectedBytes = new byte[]{0, 0, 0, 0}; // set size of zero

        testMarshalUnmarshal( Set.of(), expectedBytes );
    }

    @Test
    void singletonSet() throws Exception
    {
        var element = "Whirly Dirly";

        var expectedBytes = new byte[20];
        var buffer = ByteBuffer.wrap( expectedBytes );
        buffer.putInt( 1 ); // set size
        StringMarshal.marshal( buffer, element );

        testMarshalUnmarshal( Set.of( element ), expectedBytes );
    }

    @Test
    void multiElementSet() throws Exception
    {
        var set = Set.of( "One", "Two", "Three", "Four" );

        var expectedBytes = new byte[35];
        var buffer = ByteBuffer.wrap( expectedBytes );
        buffer.putInt( 4 ); // set size
        for ( var element : set )
        {
            StringMarshal.marshal( buffer, element );
        }

        testMarshalUnmarshal( set, expectedBytes );
    }

    private static void testMarshalUnmarshal( Set<String> originalSet, byte[] expectedBytes ) throws IOException
    {
        var outputStream = new ByteArrayOutputStream();
        var writableChannel = new OutputStreamWritableChannel( outputStream );
        SetMarshal.marshalSet( writableChannel, originalSet );

        var bytes = outputStream.toByteArray();
        assertArrayEquals( expectedBytes, bytes );

        var readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( bytes ) );
        var newSet = SetMarshal.unmarshalSet( readableChannel );
        assertEquals( originalSet, newSet );
    }
}
