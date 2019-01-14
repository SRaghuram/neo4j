/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.Address;
import akka.cluster.UniqueAddress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import org.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

@RunWith( Parameterized.class )
public class UniqueAddressMarshalTest
{
    @Parameterized.Parameter
    public UniqueAddress uniqueAddress;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<UniqueAddress> params()
    {
        return Arrays.asList(
                new UniqueAddress( new Address( "protocol", "system" ), 17L ),
                new UniqueAddress( new Address( "protocol", "system", "host", 87 ), 17L )
        );
    }

    @Test
    public void shouldMarshalAndUnMarshal() throws IOException, EndOfStreamException
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
