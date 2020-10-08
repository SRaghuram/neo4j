/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.kernel.database.DatabaseIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InefficientNamedDatabaseIdMarshalTest
{

    @Test
    void shouldMarshalAndUnmarshal() throws IOException, EndOfStreamException
    {
        var namedDatabaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );

        var marshal = InefficientNamedDatabaseIdMarshal.INSTANCE;

        var outputStream = new ByteArrayOutputStream();
        var outputStreamWritableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( namedDatabaseId, outputStreamWritableChannel );
        var unmarshal = marshal.unmarshal( new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) ) );

        assertEquals( unmarshal, namedDatabaseId );
    }
}
