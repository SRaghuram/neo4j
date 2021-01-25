/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.version;

import com.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static java.lang.Integer.parseInt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterStateVersionMarshalTest
{
    @Test
    void shouldHaveNullStartState()
    {
        var marshal = new ClusterStateVersionMarshal();

        assertNull( marshal.startState() );
    }

    @Test
    void shouldNotSupportOrdinal()
    {
        var marshal = new ClusterStateVersionMarshal();

        assertThrows( UnsupportedOperationException.class, () -> marshal.ordinal( new ClusterStateVersion( 1, 0 ) ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"0.0", "0.42", "1.0", "1.3", "42.0", "42.42", "9.99"} )
    void shouldMarshalAndUnmarshal( String versionString ) throws Exception
    {
        var version = newVersion( versionString );
        testMarshalUnmarshal( version );
    }

    private static ClusterStateVersion newVersion( String versionString )
    {
        var split = versionString.split( "\\." );
        return new ClusterStateVersion( parseInt( split[0] ), parseInt( split[1] ) );
    }

    private static void testMarshalUnmarshal( ClusterStateVersion version ) throws Exception
    {
        var marshal = new ClusterStateVersionMarshal();

        var outputStream = new ByteArrayOutputStream();
        var writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( version, writableChannel );

        var inputStream = new ByteArrayInputStream( outputStream.toByteArray() );
        var readableChannel = new InputStreamReadableChannel( inputStream );
        var newVersion = marshal.unmarshal0( readableChannel );

        assertEquals( version, newVersion );
    }
}
