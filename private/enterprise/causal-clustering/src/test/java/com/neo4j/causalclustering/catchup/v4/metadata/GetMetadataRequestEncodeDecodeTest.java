/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.metadata;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GetMetadataRequestEncodeDecodeTest
{
    @Test
    public void shouldEncodeDecode()
    {
        //given
        final var channel = new EmbeddedChannel( new GetMetadataRequestEncoder(), new GetMetadataRequestDecoder() );
        final var request = new GetMetadataRequest( "test", "all" );

        //when
        channel.writeOutbound( request );
        final var message = channel.readOutbound();
        channel.writeInbound( message );

        //then
        var received = channel.readInbound();
        assertNotNull( received );
        assertThat( received ).isInstanceOf( GetMetadataRequest.class );
    }
}
