/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.Channel;
import com.neo4j.causalclustering.protocol.ApplicationProtocolVersion;
import com.neo4j.causalclustering.protocol.Protocol;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletionException;

import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class HandshakeServerEnsureMagicTest
{
    private final ApplicationSupportedProtocols supportedApplicationProtocol =
            new ApplicationSupportedProtocols( RAFT, TestProtocols.TestApplicationProtocols.listVersionsOf( RAFT ) );

    private final Channel channel = mock( Channel.class );
    private final ApplicationProtocolRepository applicationProtocolRepository =
            new ApplicationProtocolRepository( TestProtocols.TestApplicationProtocols.values(), supportedApplicationProtocol );
    private final ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( TestProtocols.TestModifierProtocols.values(), emptyList() );

    private final HandshakeServer server = new HandshakeServer(
            applicationProtocolRepository,
            modifierProtocolRepository, channel );

    @ParameterizedTest
    @MethodSource( "messages" )
    void shouldThrowIfMagicHasNotBeenSent( ServerMessage message )
    {
        assertThrows( IllegalStateException.class, () -> message.dispatch( server ) );
    }

    @ParameterizedTest
    @MethodSource( "messages" )
    void shouldCompleteExceptionallyIfMagicHasNotBeenSent( ServerMessage message )
    {
        // when
        try
        {
            message.dispatch( server );
        }
        catch ( Exception ignored )
        {
            // swallow, tested elsewhere
        }

        // then future is completed exceptionally
        var error = assertThrows( CompletionException.class, () -> server.protocolStackFuture().getNow( null ) );
        assertThat( error.getCause(), instanceOf( ServerHandshakeException.class ) );
    }

    @ParameterizedTest
    @MethodSource( "messages" )
    void shouldNotThrowIfMagicHasBeenSent( ServerMessage message )
    {
        // given
        InitialMagicMessage.instance().dispatch( server );

        // when / then
        assertDoesNotThrow( () -> message.dispatch( server ) );
    }

    @ParameterizedTest
    @MethodSource( "messages" )
    void shouldNotCompleteExceptionallyIfMagicHasBeenSent( ServerMessage message )
    {
        // given
        InitialMagicMessage.instance().dispatch( server );

        // when
        message.dispatch( server );

        // then future should either not complete exceptionally or do so for non-magic reasons
        try
        {
            server.protocolStackFuture().getNow( null );
        }
        catch ( CompletionException ex )
        {
            assertThat( ex.getMessage(), not( containsStringIgnoringCase( "magic" ) ) );
        }
    }

    private static Collection<ServerMessage> messages()
    {
        return asList(
                new ApplicationProtocolRequest( RAFT.canonicalName(),
                        Set.of( new ApplicationProtocolVersion( 1, 0 ), new ApplicationProtocolVersion( 2, 0 ) ) ),
                new ModifierProtocolRequest( Protocol.ModifierProtocolCategory.COMPRESSION.canonicalName(), Set.of( "3", "4" ) ),
                new SwitchOverRequest( RAFT.canonicalName(), new ApplicationProtocolVersion( 2, 0 ), emptyList() )
        );
    }
}
