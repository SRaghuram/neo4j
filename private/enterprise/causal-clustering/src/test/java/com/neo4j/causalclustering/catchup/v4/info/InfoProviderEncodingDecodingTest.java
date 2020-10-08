/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.info;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.kernel.database.DatabaseIdFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class InfoProviderEncodingDecodingTest
{
    @Test
    void shouldEncodeAndDecodeRequestMessage()
    {
        var getReconciliationInfoRequest = new InfoRequest( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) );

        var embeddedChannel = new EmbeddedChannel( new InfoRequestEncoder(), new InfoRequestDecoder() );

        embeddedChannel.writeOutbound( getReconciliationInfoRequest );

        Object nextChunk;
        while ( (nextChunk = embeddedChannel.readOutbound()) != null )
        {
            embeddedChannel.writeInbound( nextChunk );
        }

        var deserialised = embeddedChannel.readInbound();

        assertThat( deserialised ).isEqualTo( getReconciliationInfoRequest );
    }

    @ParameterizedTest
    @MethodSource( "responses" )
    void shouldEncodeAndDecodeResponseMessage( InfoResponse response )
    {
        var embeddedChannel = new EmbeddedChannel( new InfoResponseEncoder(), new InfoResponseDecoder() );

        embeddedChannel.writeOutbound( response );

        Object nextChunk = null;
        while ( (nextChunk = embeddedChannel.readOutbound()) != null )
        {
            embeddedChannel.writeInbound( nextChunk );
        }

        var deserialised = embeddedChannel.readInbound();

        assertThat( deserialised ).isEqualTo( response );
    }

    static Stream<InfoResponse> responses()
    {
        return Stream.of( InfoResponse.create( 1, null ), InfoResponse.create( 1, "Failure" ) );
    }
}
