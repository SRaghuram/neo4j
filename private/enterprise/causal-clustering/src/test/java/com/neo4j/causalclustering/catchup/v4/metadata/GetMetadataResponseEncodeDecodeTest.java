/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.metadata;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class GetMetadataResponseEncodeDecodeTest
{
    @Test
    public void shouldEncodeDecode()
    {
        //given
        final var channel = new EmbeddedChannel( new GetMetadataResponseEncoder(), new GetMetadataResponseDecoder() );
        final var commands = List.of( "a", "b" );
        final var request = new GetMetadataResponse( commands );

        //when
        channel.writeOutbound( request );
        final var message = channel.readOutbound();
        channel.writeInbound( message );

        //then
        var received = channel.readInbound();
        assertNotSame( request, received );
        assertEquals( request, received );
    }
}
