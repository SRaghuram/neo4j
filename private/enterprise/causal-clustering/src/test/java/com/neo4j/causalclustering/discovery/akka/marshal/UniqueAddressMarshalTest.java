/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.Address;
import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class UniqueAddressMarshalTest
{
    static Stream<UniqueAddress> params()
    {
        return Stream.of(
                new UniqueAddress( new Address( "protocol", "system" ), 17L ),
                new UniqueAddress( new Address( "protocol", "system", "host", 87 ), 17L )
        );
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldMarshalAndUnMarshal( UniqueAddress uniqueAddress ) throws IOException, EndOfStreamException
    {
        // given
        ChannelMarshal<UniqueAddress> marshal = new UniqueAddressMarshal();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // when
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( uniqueAddress, writableChannel );

        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        UniqueAddress result = marshal.unmarshal( readableChannel );

        // then
        assertNotSame( uniqueAddress, result );
        assertEquals( uniqueAddress, result );
    }
}
