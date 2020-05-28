/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.Channel;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import com.neo4j.configuration.ApplicationProtocolVersion;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;

import org.neo4j.internal.helpers.collection.Pair;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static com.neo4j.causalclustering.protocol.handshake.StatusCode.FAILURE;
import static com.neo4j.causalclustering.protocol.handshake.StatusCode.SUCCESS;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols.RAFT_1;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZ4;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZO;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.ROT13;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.SNAPPY;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.COMPRESSION;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.GRATUITOUS_OBFUSCATION;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class HandshakeServerTest
{
    private final Channel channel = mock( Channel.class );
    private final ApplicationSupportedProtocols supportedApplicationProtocol =
            new ApplicationSupportedProtocols( RAFT, emptyList() );
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols = List.of(
            new ModifierSupportedProtocols( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ),
            new ModifierSupportedProtocols( GRATUITOUS_OBFUSCATION, TestModifierProtocols.listVersionsOf( GRATUITOUS_OBFUSCATION ) )
    );
    private final ApplicationProtocolRepository applicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedApplicationProtocol );
    private final ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), supportedModifierProtocols );

    private final HandshakeServer server =
            new HandshakeServer( applicationProtocolRepository, modifierProtocolRepository, channel );

    @Test
    void shouldDeclineUnallowedApplicationProtocol()
    {
        // when
        server.handle( new ApplicationProtocolRequest( TestApplicationProtocols.CATCHUP_1.category(),
                Set.of( TestApplicationProtocols.CATCHUP_1.implementation() ) ) );

        // then
        verify( channel ).dispose();
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackOnUnallowedApplicationProtocol()
    {
        // when
        server.handle( new ApplicationProtocolRequest( TestApplicationProtocols.CATCHUP_1.category(),
                Set.of( TestApplicationProtocols.CATCHUP_1.implementation() ) ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    void shouldSendApplicationProtocolResponseForKnownProtocol()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 1, 0 ), new ApplicationProtocolVersion( 2, 0 ), new ApplicationProtocolVersion( 3, 0 ) );

        // when
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), versions ) );

        // then
        verify( channel ).writeAndFlush(
                new ApplicationProtocolResponse( SUCCESS, TestApplicationProtocols.RAFT_3.category(), TestApplicationProtocols.RAFT_3.implementation() ) );
    }

    @Test
    void shouldNotCloseConnectionIfKnownApplicationProtocol()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 1, 0 ), new ApplicationProtocolVersion( 2, 0 ), new ApplicationProtocolVersion( 3, 0 ) );

        // when
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), versions ) );

        // then
        assertUnfinished();
    }

    @Test
    void shouldSendNegativeResponseAndCloseForUnknownApplicationProtocol()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 1, 0 ), new ApplicationProtocolVersion( 2, 0 ), new ApplicationProtocolVersion( 3, 0 ) );

        // when
        server.handle( new ApplicationProtocolRequest( "UNKNOWN", versions ) );

        // then
        var inOrder = inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( ApplicationProtocolResponse.NO_PROTOCOL );
        inOrder.verify( channel ).dispose();
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackForUnknownApplicationProtocol()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 1, 0 ), new ApplicationProtocolVersion( 2, 0 ), new ApplicationProtocolVersion( 3, 0 ) );

        // when
        server.handle( new ApplicationProtocolRequest( "UNKNOWN", versions ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    void shouldSendModifierProtocolResponseForGivenProtocol()
    {
        // given
        var versions = Set.of( TestModifierProtocols.allVersionsOf( COMPRESSION ) );

        // when
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), versions ) );

        // then
        var expected = TestModifierProtocols.latest( COMPRESSION );
        verify( channel ).writeAndFlush(
                new ModifierProtocolResponse( SUCCESS,  expected.category(), expected.implementation() ) );
    }

    @Test
    void shouldNotCloseConnectionForGivenModifierProtocol()
    {
        // given
        var versions = Set.of( SNAPPY.implementation(), LZO.implementation(), LZ4.implementation() );

        // when
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), versions ) );

        // then
        assertUnfinished();
    }

    @Test
    void shouldSendFailModifierProtocolResponseForUnknownVersion()
    {
        // given
        var versions = Set.of( "Not a real protocol" );

        // when
        var protocolName = COMPRESSION.canonicalName();
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        verify( channel ).writeAndFlush(
                new ModifierProtocolResponse( FAILURE, protocolName, "" ) );
    }

    @Test
    void shouldNotCloseConnectionIfUnknownModifierProtocolVersion()
    {
        // given
        var versions = Set.of( "not a real algorithm" );

        // when
        var protocolName = COMPRESSION.canonicalName();
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        assertUnfinished();
    }

    @Test
    void shouldSendFailModifierProtocolResponseForUnknownProtocol()
    {
        // given
        var versions = Set.of( SNAPPY.implementation(), LZO.implementation(), LZ4.implementation() );

        // when
        var protocolName = "let's just randomly reorder all the bytes";
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        verify( channel ).writeAndFlush(
                new ModifierProtocolResponse( FAILURE, protocolName, "" ) );
    }

    @Test
    void shouldNotCloseConnectionIfUnknownModifierProtocol()
    {
        // given
        var versions = Set.of( SNAPPY.implementation(), LZO.implementation(), LZ4.implementation() );

        // when
        var protocolName = "let's just randomly reorder all the bytes";
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        assertUnfinished();
    }

    @Test
    void shouldSendFailureOnUnknownProtocolSwitchOver()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );
        var unknownProtocolName = "UNKNOWN";
        server.handle( new ApplicationProtocolRequest( unknownProtocolName, Set.of( version ) ) );

        // when
        server.handle( new SwitchOverRequest( unknownProtocolName, version, emptyList() ) );

        // then
        var inOrder = inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackOnUnknownProtocolSwitchOver()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );
        var unknownProtocolName = "UNKNOWN";
        server.handle( new ApplicationProtocolRequest( unknownProtocolName, Set.of( version ) ) );

        // when
        server.handle( new SwitchOverRequest( unknownProtocolName, version, emptyList() ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    void shouldSendFailureIfSwitchOverBeforeNegotiation()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.category(), version, emptyList() ) );

        // then
        var inOrder = inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackIfSwitchOverBeforeNegotiation()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.category(), version, emptyList() ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    void shouldSendFailureIfSwitchOverDiffersFromNegotiatedProtocol()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( version ) ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.category(), nextMajorVersion( version ), emptyList() ) );

        // then
        var inOrder = inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackIfSwitchOverDiffersFromNegotiatedProtocol()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( version ) ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.category(), nextMajorVersion( version ), emptyList() ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    void shouldSendFailureIfSwitchOverDiffersByNameFromNegotiatedModifierProtocol()
    {
        // given
        var modifierVersion = ROT13.implementation();
        var version = new ApplicationProtocolVersion( 1, 0 );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), Set.of( modifierVersion ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(), version,
                List.of( Pair.of( GRATUITOUS_OBFUSCATION.canonicalName(), modifierVersion ) ) ) );

        // then
        var inOrder = inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackIfSwitchOverDiffersByNameFromNegotiatedModifiedProtocol()
    {
        // given
        var modifierVersion = ROT13.implementation();
        var version = new ApplicationProtocolVersion( 1, 0 );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), Set.of( modifierVersion ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                version,
                List.of( Pair.of( GRATUITOUS_OBFUSCATION.canonicalName(), modifierVersion ) ) ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    void shouldSendFailureIfSwitchOverChangesOrderOfModifierProtocols()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), Set.of( SNAPPY.implementation() ) ) );
        server.handle( new ModifierProtocolRequest( GRATUITOUS_OBFUSCATION.canonicalName(), Set.of( ROT13.implementation() ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                version,
                List.of( Pair.of( GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() ),
                        Pair.of( COMPRESSION.canonicalName(), SNAPPY.implementation() ) ) ) );

        // then
        var inOrder = inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackIfSwitchOverChangesOrderOfModifierProtocols()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), Set.of( SNAPPY.implementation() ) ) );
        server.handle( new ModifierProtocolRequest( GRATUITOUS_OBFUSCATION.canonicalName(), Set.of( ROT13.implementation() ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                version,
                List.of( Pair.of( GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() ),
                        Pair.of( COMPRESSION.canonicalName(), SNAPPY.implementation() ) ) ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    void shouldSendFailureIfSwitchOverDiffersByVersionFromNegotiatedModifierProtocol()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), Set.of( SNAPPY.implementation() ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT_1.category(),
                version,
                List.of( Pair.of( COMPRESSION.canonicalName(), LZ4.implementation() ) )
        ) );

        // then
        var inOrder = inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackIfSwitchOverDiffersByVersionFromNegotiatedModifiedProtocol()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), Set.of( SNAPPY.implementation() ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT_1.category(),
                version,
                List.of( Pair.of( COMPRESSION.canonicalName(), LZ4.implementation() ) )
        ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    void shouldCompleteProtocolStackOnSuccessfulSwitchOverWithNoModifierProtocols()
    {
        // given
        var version = new ApplicationProtocolVersion( 1, 0 );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( version ) ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.category(), version, emptyList() ) );

        // then
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        var protocolStack = server.protocolStackFuture().getNow( null );
        assertThat( protocolStack, equalTo( new ProtocolStack( RAFT_1, emptyList() ) ) );
    }

    @Test
    void shouldCompleteProtocolStackOnSuccessfulSwitchOverWithModifierProtocols()
    {
        // given
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( RAFT_1.implementation() ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), Set.of( SNAPPY.implementation() ) ) );
        server.handle( new ModifierProtocolRequest( GRATUITOUS_OBFUSCATION.canonicalName(), Set.of( ROT13.implementation() ) ) );

        // when
        var modifierRequest = List.of(
                Pair.of( SNAPPY.category(), SNAPPY.implementation() ),
                Pair.of( ROT13.category(), ROT13.implementation() )
        );
        server.handle( new SwitchOverRequest( RAFT_1.category(), RAFT_1.implementation(), modifierRequest ) );

        // then
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        var protocolStack = server.protocolStackFuture().getNow( null );
        var modifiers = List.<ModifierProtocol>of( SNAPPY, ROT13 );
        assertThat( protocolStack, equalTo( new ProtocolStack( RAFT_1, modifiers ) ) );
    }

    @Test
    void shouldCompleteProtocolStackOnSuccessfulSwitchOverWithConfiguredModifierProtocols()
    {
        // given
        var requestedVersions = Set.of( TestModifierProtocols.allVersionsOf( COMPRESSION ) );
        var expectedNegotiatedVersion = SNAPPY.implementation();
        var configuredVersions = singletonList( expectedNegotiatedVersion );

        var supportedModifierProtocols =
                List.of( new ModifierSupportedProtocols( COMPRESSION, configuredVersions ) );

        var modifierProtocolRepository =
                new ModifierProtocolRepository( TestModifierProtocols.values(), supportedModifierProtocols );

        var server = new HandshakeServer( applicationProtocolRepository, modifierProtocolRepository, channel );

        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), Set.of( RAFT_1.implementation() ) ) );
        server.handle( new ModifierProtocolRequest(
                COMPRESSION.canonicalName(),
                requestedVersions ) );

        // when
        var modifierRequest = List.of( Pair.of( SNAPPY.category(), SNAPPY.implementation() ) );
        server.handle( new SwitchOverRequest( RAFT_1.category(), RAFT_1.implementation(), modifierRequest ) );

        // then
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        var protocolStack = server.protocolStackFuture().getNow( null );
        var modifiers = List.<ModifierProtocol>of( SNAPPY );
        assertThat( protocolStack, equalTo( new ProtocolStack( RAFT_1, modifiers ) ) );
    }

    @Test
    void shouldSuccessfullySwitchOverWhenServerHasConfiguredRaftVersions()
    {
        // given
        var requestedVersions = Set.of( TestApplicationProtocols.allVersionsOf( RAFT ) );
        var expectedNegotiatedVersion = new ApplicationProtocolVersion( 1, 0 );
        var applicationProtocolRepository = new ApplicationProtocolRepository(
                TestApplicationProtocols.values(), new ApplicationSupportedProtocols( RAFT, singletonList( expectedNegotiatedVersion ) ) );

        var server = new HandshakeServer( applicationProtocolRepository, modifierProtocolRepository, channel );

        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), requestedVersions ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.category(), expectedNegotiatedVersion, emptyList() ) );

        // then
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        var protocolStack = server.protocolStackFuture().getNow( null );
        var expectedProtocolStack = new ProtocolStack(
                applicationProtocolRepository.select( RAFT.canonicalName(), expectedNegotiatedVersion ).orElseThrow(),
                emptyList() );
        assertThat( protocolStack, equalTo( expectedProtocolStack ) );
    }

    private void assertUnfinished()
    {
        verify( channel, never() ).dispose();
        assertFalse( server.protocolStackFuture().isDone() );
    }

    private void assertExceptionallyCompletedProtocolStackFuture()
    {
        assertTrue( server.protocolStackFuture().isCompletedExceptionally() );
        var ex = assertThrows( CompletionException.class, () -> server.protocolStackFuture().getNow( null ) );
        assertThat( ex.getCause(), instanceOf( ServerHandshakeException.class ) );
    }

    private static ApplicationProtocolVersion nextMajorVersion( ApplicationProtocolVersion version )
    {
        return new ApplicationProtocolVersion( version.major() + 1, 0 );
    }
}
