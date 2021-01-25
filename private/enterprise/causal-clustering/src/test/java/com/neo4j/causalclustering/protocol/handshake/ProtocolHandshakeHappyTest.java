/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.Channel;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols.RAFT_1;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZ4;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZO;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.SNAPPY;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.COMPRESSION;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.GRATUITOUS_OBFUSCATION;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

/**
 * @see ProtocolHandshakeSadTest sad path tests
 */
class ProtocolHandshakeHappyTest
{
    static Collection<Parameters> data()
    {
        // Application protocols
        var allRaft =
                new ApplicationSupportedProtocols( RAFT, TestApplicationProtocols.listVersionsOf( RAFT ) );
        var raft1 =
                new ApplicationSupportedProtocols( RAFT, singletonList( RAFT_1.implementation() ) );
        var allRaftByDefault =
                new ApplicationSupportedProtocols( RAFT, emptyList() );

        // Modifier protocols
        var allModifiers = asList(
                new ModifierSupportedProtocols( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ),
                new ModifierSupportedProtocols( GRATUITOUS_OBFUSCATION, TestModifierProtocols.listVersionsOf( GRATUITOUS_OBFUSCATION ) )
                );
        var allCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ) );
        var allObfuscationModifiers = singletonList(
                new ModifierSupportedProtocols( GRATUITOUS_OBFUSCATION, TestModifierProtocols.listVersionsOf( GRATUITOUS_OBFUSCATION ) ) );
        var allCompressionModifiersByDefault = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, emptyList() ) );

        var onlyLzoCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, singletonList( LZO.implementation() ) ) );
        var onlySnappyCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, singletonList( SNAPPY.implementation() ) ) );

        Collection<ModifierSupportedProtocols> noModifiers = emptyList();

        // Ordered modifier protocols
        var modifierProtocolRepository = new ModifierProtocolRepository( TestModifierProtocols.values(), allModifiers );
        var lzoFirstVersions = new String[] { LZO.implementation(), LZ4.implementation(), SNAPPY.implementation() };
        var lzoFirstCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, asList( lzoFirstVersions ) ) );
        var preferredLzoFirstCompressionModifier =
                modifierProtocolRepository.select( COMPRESSION.canonicalName(), asSet( lzoFirstVersions ) ).get();

        var snappyFirstVersions = new String[] { SNAPPY.implementation(), LZ4.implementation(), LZO.implementation() };
        var snappyFirstCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, asList( snappyFirstVersions ) ) );
        var preferredSnappyFirstCompressionModifier =
                modifierProtocolRepository.select( COMPRESSION.canonicalName(), asSet( snappyFirstVersions ) ).get();

        return asList(
                // Everything
                new Parameters( allRaft, allRaft, allModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),

                // Application protocols
                new Parameters( allRaft, allRaftByDefault, allModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),
                new Parameters( allRaftByDefault, allRaft, allModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),

                new Parameters( allRaft, raft1, allModifiers, allModifiers, RAFT_1,
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),
                new Parameters( raft1, allRaft, allModifiers, allModifiers, RAFT_1,
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),

                // Modifier protocols
                new Parameters( allRaft, allRaft, allModifiers, allCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ) } ),
                new Parameters( allRaft, allRaft, allCompressionModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ) } ),
                new Parameters( allRaft, allRaft, allModifiers, allCompressionModifiersByDefault, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ) } ),
                new Parameters( allRaft, allRaft, allCompressionModifiersByDefault, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ) } ),
                new Parameters( allRaft, allRaft, allModifiers, allObfuscationModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {  TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),
                new Parameters( allRaft, allRaft, allObfuscationModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {  TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),

                // prioritisation
                new Parameters( allRaft, allRaft, allModifiers, lzoFirstCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { LZO } ),
                new Parameters( allRaft, allRaft, lzoFirstCompressionModifiers, allCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { preferredLzoFirstCompressionModifier } ),
                new Parameters( allRaft, allRaft, allModifiers, snappyFirstCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { SNAPPY } ),
                new Parameters( allRaft, allRaft, snappyFirstCompressionModifiers, allCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { preferredSnappyFirstCompressionModifier } ),

                // restriction
                new Parameters( allRaft, allRaft, allModifiers, onlyLzoCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { LZO } ),
                new Parameters( allRaft, allRaft, onlyLzoCompressionModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { LZO } ),

                // incompatible
                new Parameters( allRaft, allRaft, onlySnappyCompressionModifiers, onlyLzoCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {} ),
                new Parameters( allRaft, allRaft, onlyLzoCompressionModifiers, onlySnappyCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {} ),

                // no modifiers
                new Parameters( allRaft, allRaft, allModifiers, noModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {} ),
                new Parameters( allRaft, allRaft, noModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {} )
                );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldHandshakeApplicationProtocolOnClient( Parameters parameters )
    {
        // given
        var fixture = new Fixture( parameters );

        // when
        var clientHandshakeFuture = fixture.initiate();

        // then
        assertFalse( fixture.clientChannel.isClosed() );
        var clientProtocolStack = clientHandshakeFuture.getNow( null );
        assertThat( clientProtocolStack.applicationProtocol() ).isEqualTo( parameters.expectedApplicationProtocol );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldHandshakeModifierProtocolsOnClient( Parameters parameters )
    {
        // given
        var fixture = new Fixture( parameters );

        // when
        var clientHandshakeFuture = fixture.initiate();

        // then
        assertFalse( fixture.clientChannel.isClosed() );
        var clientProtocolStack = clientHandshakeFuture.getNow( null );
        if ( parameters.expectedModifierProtocols.length == 0 )
        {
            assertThat( clientProtocolStack.modifierProtocols() ).isEmpty();
        }
        else
        {
            assertThat( clientProtocolStack.modifierProtocols() ).contains( parameters.expectedModifierProtocols );
        }
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldHandshakeApplicationProtocolOnServer( Parameters parameters )
    {
        // given
        var fixture = new Fixture( parameters );

        // when
        fixture.initiate();
        fixture.handshakeServer.protocolStackFuture();
        var serverHandshakeFuture = fixture.handshakeServer.protocolStackFuture();

        // then
        assertFalse( fixture.clientChannel.isClosed() );
        var serverProtocolStack = serverHandshakeFuture.getNow( null );
        assertThat( serverProtocolStack.applicationProtocol() ).isEqualTo( parameters.expectedApplicationProtocol );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldHandshakeModifierProtocolsOnServer( Parameters parameters )
    {
        // given
        var fixture = new Fixture( parameters );

        // when
        fixture.initiate();
        fixture.handshakeServer.protocolStackFuture();
        var serverHandshakeFuture = fixture.handshakeServer.protocolStackFuture();

        // then
        assertFalse( fixture.clientChannel.isClosed() );
        var serverProtocolStack = serverHandshakeFuture.getNow( null );
        if ( parameters.expectedModifierProtocols.length == 0 )
        {
            assertThat( serverProtocolStack.modifierProtocols() ).isEmpty();
        }
        else
        {
            assertThat( serverProtocolStack.modifierProtocols() ).contains( parameters.expectedModifierProtocols );
        }
    }

    static class Fixture
    {
        final HandshakeClient handshakeClient;
        final HandshakeServer handshakeServer;
        final FakeChannelWrapper clientChannel;
        final ApplicationProtocolRepository clientApplicationProtocolRepository;
        final ModifierProtocolRepository clientModifierProtocolRepository;
        final Parameters parameters;

        Fixture( Parameters parameters )
        {
            var serverApplicationProtocolRepository =
                    new ApplicationProtocolRepository( TestApplicationProtocols.values(), parameters.serverApplicationProtocol );
            var serverModifierProtocolRepository =
                    new ModifierProtocolRepository( TestModifierProtocols.values(), parameters.serverModifierProtocols );

            clientApplicationProtocolRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), parameters.clientApplicationProtocol );
            clientModifierProtocolRepository = new ModifierProtocolRepository( TestModifierProtocols.values(), parameters.clientModifierProtocols );

            handshakeClient = new HandshakeClient( new CompletableFuture<>() );
            handshakeServer = new HandshakeServer(
                    serverApplicationProtocolRepository,
                    serverModifierProtocolRepository,
                    new FakeServerChannel( handshakeClient ) );
            clientChannel = new FakeClientChannel( handshakeServer );
            this.parameters = parameters;
        }

        private CompletableFuture<ProtocolStack> initiate()
        {
            handshakeClient.initiate( clientChannel, clientApplicationProtocolRepository, clientModifierProtocolRepository );
            return handshakeClient.protocol();
        }
    }

    static class Parameters
    {
        final ApplicationSupportedProtocols clientApplicationProtocol;
        final ApplicationSupportedProtocols serverApplicationProtocol;
        final Collection<ModifierSupportedProtocols> clientModifierProtocols;
        final Collection<ModifierSupportedProtocols> serverModifierProtocols;
        final ApplicationProtocol expectedApplicationProtocol;
        final ModifierProtocol[] expectedModifierProtocols;

        Parameters( ApplicationSupportedProtocols clientApplicationProtocol,
                ApplicationSupportedProtocols serverApplicationProtocol,
                Collection<ModifierSupportedProtocols> clientModifierProtocols,
                Collection<ModifierSupportedProtocols> serverModifierProtocols,
                ApplicationProtocol expectedApplicationProtocol,
                ModifierProtocol[] expectedModifierProtocols )
        {
            this.clientModifierProtocols = clientModifierProtocols;
            this.clientApplicationProtocol = clientApplicationProtocol;
            this.serverApplicationProtocol = serverApplicationProtocol;
            this.serverModifierProtocols = serverModifierProtocols;
            this.expectedApplicationProtocol = expectedApplicationProtocol;
            this.expectedModifierProtocols = expectedModifierProtocols;
        }
    }

    abstract static class FakeChannelWrapper implements Channel
    {
        private boolean closed;

        @Override
        public boolean isDisposed()
        {
            return closed;
        }

        @Override
        public void dispose()
        {
            closed = true;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public abstract CompletableFuture<Void> write( Object msg );

        @Override
        public CompletableFuture<Void> writeAndFlush( Object msg )
        {
            return write( msg );
        }

        boolean isClosed()
        {
            return closed;
        }
    }

    static class FakeServerChannel extends FakeChannelWrapper
    {
        private final HandshakeClient handshakeClient;

        FakeServerChannel( HandshakeClient handshakeClient )
        {
            super();
            this.handshakeClient = handshakeClient;
        }

        @Override
        public CompletableFuture<Void> write( Object msg )
        {
            ((ClientMessage) msg).dispatch( handshakeClient );
            return CompletableFuture.completedFuture( null );
        }
    }

    static class FakeClientChannel extends FakeChannelWrapper
    {
        private final HandshakeServer handshakeServer;

        FakeClientChannel( HandshakeServer handshakeServer )
        {
            super();
            this.handshakeServer = handshakeServer;
        }

        @Override
        public CompletableFuture<Void> write( Object msg )
        {
            ((ServerMessage) msg).dispatch( handshakeServer );
            return CompletableFuture.completedFuture( null );
        }
    }
}
