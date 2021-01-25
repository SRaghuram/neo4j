/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.configuration.ApplicationProtocolVersion;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.neo4j.internal.helpers.collection.Pair;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ServerMessageEncodingTest
{
    private final ClientMessageEncoder encoder = new ClientMessageEncoder();
    private final ServerMessageDecoder decoder = new ServerMessageDecoder();

    @ParameterizedTest
    @MethodSource( "responseMessages" )
    void shouldCompleteEncodingRoundTrip( ServerMessage message )
    {
        //when
        var output = encodeDecode( message );

        //then
        assertEquals( List.of( message ), output );
    }

    private List<Object> encodeDecode( ServerMessage message )
    {
        var byteBuf = Unpooled.buffer();
        var output = new ArrayList<>();

        encoder.encode( null, message, byteBuf );
        decoder.decode( null, byteBuf, output );

        return output;
    }

    private static Collection<ServerMessage> responseMessages()
    {
        return List.of(
                new ApplicationProtocolRequest( "protocol",
                        Set.of( new ApplicationProtocolVersion( 3, 0 ), new ApplicationProtocolVersion( 7, 0 ), new ApplicationProtocolVersion( 13, 0 ) ) ),
                new ModifierProtocolRequest( "modifierProtocol", Set.of( "Foo", "Bar", "Baz" ) ),
                new SwitchOverRequest( "protocol", new ApplicationProtocolVersion( 38, 0 ), emptyList() ),
                new SwitchOverRequest( "protocol", new ApplicationProtocolVersion( 38, 0 ),
                        List.of( Pair.of( "mod1", "Foo" ), Pair.of( "mod2", "Quux" ) ) )
        );
    }
}
