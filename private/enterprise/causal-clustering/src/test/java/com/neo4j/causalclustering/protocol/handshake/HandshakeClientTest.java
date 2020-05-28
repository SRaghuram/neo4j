/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.Channel;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory;
import com.neo4j.configuration.ApplicationProtocolVersion;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.ROT13;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.SNAPPY;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class HandshakeClientTest
{
    private final HandshakeClient client = new HandshakeClient( new CompletableFuture<>() );
    private final Channel channel = mock( Channel.class );
    private final ApplicationProtocolCategory applicationProtocolIdentifier = RAFT;
    private final ApplicationSupportedProtocols supportedApplicationProtocol =
            new ApplicationSupportedProtocols( applicationProtocolIdentifier, emptyList() );
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols = Stream.of( ModifierProtocolCategory.values() )
            .map( id -> new ModifierSupportedProtocols( id, emptyList() ) )
            .collect( Collectors.toList() );
    private final ApplicationProtocolRepository applicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedApplicationProtocol );
    private final ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), supportedModifierProtocols );
    private final ApplicationProtocolVersion raftVersion = TestApplicationProtocols.latest( RAFT ).implementation();
    private final ApplicationProtocol expectedApplicationProtocol =
            applicationProtocolRepository.select( applicationProtocolIdentifier.canonicalName(), raftVersion ).orElseThrow();

    //TODO: Test for concurrent handshakes (i.e. have we respected Sharable?)

    @Test
    void shouldSendApplicationProtocolRequestOnInitiation()
    {
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        var expectedMessage = new ApplicationProtocolRequest(
                applicationProtocolIdentifier.canonicalName(),
                applicationProtocolRepository.getAll( applicationProtocolIdentifier, emptyList() ).versions()
        );

        verify( channel ).writeAndFlush( expectedMessage );
    }

    @Test
    void shouldSendModifierProtocolRequestsOnInitiation()
    {
        // when
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        // then
        Stream.of( ModifierProtocolCategory.values() ).forEach( modifierProtocolIdentifier ->
                {
                    var versions = modifierProtocolRepository.getAll( modifierProtocolIdentifier, emptyList() ).versions();
                    verify( channel ).write( new ModifierProtocolRequest( modifierProtocolIdentifier.canonicalName(), versions ) );
                } );
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackWhenApplicationProtocolResponseNotSuccessful()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.FAILURE, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackWhenApplicationProtocolResponseForIncorrectProtocol()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, "zab", raftVersion ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackWhenApplicationProtocolResponseForUnsupportedVersion()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        // when
        var version = new ApplicationProtocolVersion( Integer.MAX_VALUE, 0 );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), version ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    void shouldSendSwitchOverRequestIfNoModifierProtocolsToRequestOnApplicationProtocolResponse()
    {
        var repo = new ModifierProtocolRepository( TestModifierProtocols.values(), emptyList() );
        // given
        client.initiate( channel, applicationProtocolRepository, repo );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // then
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, emptyList() ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    void shouldNotSendSwitchOverRequestOnModifierProtocolResponseIfNotAllModifierProtocolResponsesReceived()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), "woot" ) );

        // then
        verify( channel, never() ).writeAndFlush( any( SwitchOverRequest.class ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    void shouldNotSendSwitchOverRequestIfApplicationProtocolResponseNotReceivedOnModifierProtocolResponseReceive()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );

        // then
        verify( channel, never() ).writeAndFlush( any( SwitchOverRequest.class ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    void shouldSendSwitchOverRequestOnModifierProtocolResponseIfAllModifierProtocolResponsesReceived()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() ) );

        // then
        var switchOverModifierProtocols = asList(
                Pair.of( ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ),
                Pair.of( ModifierProtocolCategory.GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() )
        );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    void shouldNotIncludeModifierProtocolInSwitchOverRequestIfNotSuccessful()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        client.handle( new ModifierProtocolResponse(
                StatusCode.FAILURE, ModifierProtocolCategory.GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() ) );

        // then
        var switchOverModifierProtocols =
                singletonList( Pair.of( ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    void shouldNotIncludeModifierProtocolInSwitchOverRequestIfUnsupportedProtocol()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, "not a protocol", "not an implementation" ) );

        // then
        var switchOverModifierProtocols =
                singletonList( Pair.of( ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    void shouldNotIncludeModifierProtocolInSwitchOverRequestIfUnsupportedVersion()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.GRATUITOUS_OBFUSCATION.canonicalName(), "Rearrange the bytes at random" ) );

        // then
        var switchOverModifierProtocols =
                singletonList( Pair.of( ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackWhenSwitchOverResponseNotSuccess()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new SwitchOverResponse( StatusCode.FAILURE ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    void shouldExceptionallyCompleteProtocolStackWhenProtocolStackNotSet()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        // when
        client.handle( new SwitchOverResponse( StatusCode.SUCCESS ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    void shouldCompleteProtocolStackOnSwitchoverResponse()
    {
        // given
        var repo = new ModifierProtocolRepository(
                TestModifierProtocols.values(),
                singletonList( new ModifierSupportedProtocols( ModifierProtocolCategory.COMPRESSION, emptyList() ) ) );

        client.initiate( channel, applicationProtocolRepository, repo );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );
        client.handle(
                new ModifierProtocolResponse( StatusCode.SUCCESS, SNAPPY.category(), SNAPPY.implementation() ) );

        // when
        client.handle( new SwitchOverResponse( StatusCode.SUCCESS ) );

        // then
        var protocolStack = client.protocol().getNow( null );
        assertThat( protocolStack, equalTo( new ProtocolStack( expectedApplicationProtocol, singletonList( SNAPPY ) ) ) );
    }

    private static void assertCompletedExceptionally( CompletableFuture<ProtocolStack> protocolStackCompletableFuture )
    {
        assertTrue( protocolStackCompletableFuture.isCompletedExceptionally() );
        var error = assertThrows( CompletionException.class, () -> protocolStackCompletableFuture.getNow( null ) );
        assertThat( error.getCause(), instanceOf( ClientHandshakeException.class ) );
    }
}
