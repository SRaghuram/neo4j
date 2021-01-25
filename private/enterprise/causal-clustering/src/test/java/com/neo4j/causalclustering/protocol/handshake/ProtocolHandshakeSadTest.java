/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import com.neo4j.configuration.ApplicationProtocolVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.CATCHUP;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @see ProtocolHandshakeHappyTest happy path tests
 */
class ProtocolHandshakeSadTest
{
    private final HandshakeClient handshakeClient = new HandshakeClient( new CompletableFuture<>() );

    private final Collection<ModifierSupportedProtocols> noModifiers = emptyList();
    private final ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( TestModifierProtocols.values(), noModifiers );

    @ParameterizedTest
    @MethodSource( "incompatibleRepositories" )
    void shouldFailClientHandshakeOnMismatchedProtocol( ApplicationProtocolRepository repository1, ApplicationProtocolRepository repository2 )
    {
        // given
        var handshakeServer = new HandshakeServer( repository1, modifierProtocolRepository,
                new ProtocolHandshakeHappyTest.FakeServerChannel( handshakeClient ) );
        var clientChannel = new ProtocolHandshakeHappyTest.FakeClientChannel( handshakeServer );

        // when
        handshakeClient.initiate( clientChannel, repository2, modifierProtocolRepository );

        // then
        var error = assertThrows( CompletionException.class, () -> handshakeClient.protocol().getNow( null ) );
        assertThat( error.getCause(), instanceOf( ClientHandshakeException.class ) );
    }

    @ParameterizedTest
    @MethodSource( "incompatibleRepositories" )
    void shouldFailHandshakeForUnknownProtocolOnServer( ApplicationProtocolRepository repository1, ApplicationProtocolRepository repository2 )
    {
        // given
        var handshakeServer = new HandshakeServer( repository1, modifierProtocolRepository,
                new ProtocolHandshakeHappyTest.FakeServerChannel( handshakeClient ) );
        var clientChannel = new ProtocolHandshakeHappyTest.FakeClientChannel( handshakeServer );

        // when
        handshakeClient.initiate( clientChannel, repository2, modifierProtocolRepository );
        var serverHandshakeFuture = handshakeServer.protocolStackFuture();

        // then
        var error = assertThrows( CompletionException.class, () -> serverHandshakeFuture.getNow( null ) );
        assertThat( error.getCause(), instanceOf( ServerHandshakeException.class ) );
    }

    private static Stream<Arguments> incompatibleRepositories()
    {
        var supportsAllRaft = new ApplicationSupportedProtocols( RAFT, emptyList() );
        var supportsAllCatchup = new ApplicationSupportedProtocols( CATCHUP, emptyList() );
        var supportsCatchup1 = new ApplicationSupportedProtocols( CATCHUP, List.of( new ApplicationProtocolVersion( 1, 0 ) ) );
        var supportsCatchup2 = new ApplicationSupportedProtocols( CATCHUP, List.of( new ApplicationProtocolVersion( 2, 0 ) ) );

        var raftProtocolsRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportsAllRaft );
        var catchupProtocolsRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportsAllCatchup );
        var catchupV1ProtocolRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportsCatchup1 );
        var catchupV2ProtocolRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportsCatchup2 );

        return Stream.of(
                Arguments.of( catchupProtocolsRepository, raftProtocolsRepository ),
                Arguments.of( raftProtocolsRepository, catchupProtocolsRepository ),
                Arguments.of( catchupV1ProtocolRepository, catchupV2ProtocolRepository ),
                Arguments.of( catchupV2ProtocolRepository, catchupV1ProtocolRepository ) );
    }
}
