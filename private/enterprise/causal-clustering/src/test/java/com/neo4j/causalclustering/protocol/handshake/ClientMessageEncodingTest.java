/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.configuration.ApplicationProtocolVersion;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ClientMessageEncodingTest
{
    private final ServerMessageEncoder encoder = new ServerMessageEncoder();
    private final ClientMessageDecoder decoder = new ClientMessageDecoder();

    private List<Object> encodeDecode( ClientMessage message ) throws ClientHandshakeException
    {
        var byteBuf = Unpooled.directBuffer();
        var output = new ArrayList<>();

        encoder.encode( null, message, byteBuf );
        decoder.decode( null, byteBuf, output );

        return output;
    }

    static Collection<ClientMessage> data()
    {
        return Arrays.asList(
                new ApplicationProtocolResponse( StatusCode.FAILURE, "protocol", new ApplicationProtocolVersion( 13, 0 ) ),
                new ModifierProtocolResponse( StatusCode.SUCCESS, "modifier", "7" ),
                new SwitchOverResponse( StatusCode.FAILURE )
                );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldCompleteEncodingRoundTrip( ClientMessage message ) throws ClientHandshakeException
    {
        //when
        var output = encodeDecode( message );

        //then
        assertThat( output ).hasSize( 1 );
        assertThat( output.get( 0 ) ).isEqualTo( message );
    }
}
