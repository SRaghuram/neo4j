/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GetAllDatabaseIdsRequestEncodeDecodeTest
{
    @Test
    public void shouldEncodeDecode()
    {
        //given
        final var channel = new EmbeddedChannel( new GetAllDatabaseIdsRequestEncoder(), new GetAllDatabaseIdsRequestDecoder() );
        final var request = new GetAllDatabaseIdsRequest();

        //when
        channel.writeOutbound( request );
        final var message = channel.readOutbound();
        channel.writeInbound( message );

        //then
        var received = channel.readInbound();
        assertNotNull( received );
        assertThat(received).isInstanceOf( GetAllDatabaseIdsRequest.class );
    }
}
