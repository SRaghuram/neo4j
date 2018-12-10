/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.helpers.collection.Pair;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith( Parameterized.class )
public class ServerMessageEncodingTest
{
    private final ServerMessage message;
    private final ClientMessageEncoder encoder = new ClientMessageEncoder();
    private final ServerMessageDecoder decoder = new ServerMessageDecoder();

    private List<Object> encodeDecode( ServerMessage message )
    {
        ByteBuf byteBuf = Unpooled.directBuffer();
        List<Object> output = new ArrayList<>();

        encoder.encode( null, message, byteBuf );
        decoder.decode( null, byteBuf, output );

        return output;
    }

    @Parameterized.Parameters( name = "ResponseMessage-{0}" )
    public static Collection<ServerMessage> data()
    {
        return asList(
                new ApplicationProtocolRequest( "protocol", asSet( 3,7,13 ) ),
                new InitialMagicMessage( "Magic string" ),
                new ModifierProtocolRequest( "modifierProtocol", asSet( "Foo", "Bar", "Baz" ) ),
                new SwitchOverRequest( "protocol", 38, emptyList() ),
                new SwitchOverRequest( "protocol", 38,
                        asList( Pair.of( "mod1", "Foo" ), Pair.of( "mod2" , "Quux" ) ) )
                );
    }

    public ServerMessageEncodingTest( ServerMessage message )
    {
        this.message = message;
    }

    @Test
    public void shouldCompleteEncodingRoundTrip()
    {
        //when
        List<Object> output = encodeDecode( message );

        //then
        assertThat( output, hasSize( 1 ) );
        assertThat( output.get( 0 ), equalTo( message ) );
    }
}
