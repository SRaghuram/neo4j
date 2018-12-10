/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.Channel;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.Pair;

import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory;
import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static com.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.ROT13;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.SNAPPY;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @see HandshakeClientEnsureMagicTest
 */
public class HandshakeClientTest
{
    private HandshakeClient client = new HandshakeClient();
    private Channel channel = mock( Channel.class );
    private ApplicationProtocolCategory applicationProtocolIdentifier = RAFT;
    private ApplicationSupportedProtocols supportedApplicationProtocol =
            new ApplicationSupportedProtocols( applicationProtocolIdentifier, emptyList() );
    private Collection<ModifierSupportedProtocols> supportedModifierProtocols = Stream.of( ModifierProtocolCategory.values() )
            .map( id -> new ModifierSupportedProtocols( id, emptyList() ) )
            .collect( Collectors.toList() );
    private ApplicationProtocolRepository applicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedApplicationProtocol );
    private ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), supportedModifierProtocols );
    private int raftVersion = TestApplicationProtocols.latest( RAFT ).implementation();
    private ApplicationProtocol expectedApplicationProtocol =
            applicationProtocolRepository.select( applicationProtocolIdentifier.canonicalName(), raftVersion ).get();

    //TODO: Test for concurrent handshakes (i.e. have we respected Sharable?)

    @Test
    public void shouldSendInitialMagicOnInitiation()
    {
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        verify( channel ).write( InitialMagicMessage.instance() );
    }

    @Test
    public void shouldSendApplicationProtocolRequestOnInitiation()
    {
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        ApplicationProtocolRequest expectedMessage = new ApplicationProtocolRequest(
                applicationProtocolIdentifier.canonicalName(),
                applicationProtocolRepository.getAll( applicationProtocolIdentifier, emptyList() ).versions()
        );

        verify( channel ).writeAndFlush( expectedMessage );
    }

    @Test
    public void shouldSendModifierProtocolRequestsOnInitiation()
    {
        // when
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        // then
        Stream.of( ModifierProtocolCategory.values() ).forEach( modifierProtocolIdentifier ->
                {
                    Set<String> versions = modifierProtocolRepository.getAll( modifierProtocolIdentifier, emptyList() ).versions();
                    verify( channel ).write( new ModifierProtocolRequest( modifierProtocolIdentifier.canonicalName(), versions ) );
                } );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackOnReceivingIncorrectMagic()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        // when
        client.handle( new InitialMagicMessage( "totally legit" ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    public void shouldAcceptCorrectMagic()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );

        // when
        client.handle( InitialMagicMessage.instance() );

        // then
        assertFalse( client.protocol().isDone() );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenApplicationProtocolResponseNotSuccessful()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.FAILURE, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenApplicationProtocolResponseForIncorrectProtocol()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, "zab", raftVersion ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenApplicationProtocolResponseForUnsupportedVersion()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), Integer.MAX_VALUE ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    public void shouldSendSwitchOverRequestIfNoModifierProtocolsToRequestOnApplicationProtocolResponse()
    {
        ModifierProtocolRepository repo = new ModifierProtocolRepository( TestModifierProtocols.values(), emptyList() );
        // given
        client.initiate( channel, applicationProtocolRepository, repo );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // then
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, emptyList() ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    public void shouldNotSendSwitchOverRequestOnModifierProtocolResponseIfNotAllModifierProtocolResponsesReceived()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), "woot" ) );

        // then
        verify( channel, never() ).writeAndFlush( any( SwitchOverRequest.class ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    public void shouldNotSendSwitchOverRequestIfApplicationProtocolResponseNotReceivedOnModifierProtocolResponseReceive()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );

        // then
        verify( channel, never() ).writeAndFlush( any( SwitchOverRequest.class ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    public void shouldSendSwitchOverRequestOnModifierProtocolResponseIfAllModifierProtocolResponsesReceived()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() ) );

        // then
        List<Pair<String,String>> switchOverModifierProtocols = asList(
                Pair.of( ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ),
                Pair.of( ModifierProtocolCategory.GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() )
        );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    public void shouldNotIncludeModifierProtocolInSwitchOverRequestIfNotSuccessful()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        client.handle( new ModifierProtocolResponse(
                StatusCode.FAILURE, ModifierProtocolCategory.GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() ) );

        // then
        List<Pair<String,String>> switchOverModifierProtocols =
                singletonList( Pair.of( ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    public void shouldNotIncludeModifierProtocolInSwitchOverRequestIfUnsupportedProtocol()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, "not a protocol", "not an implementation" ) );

        // then
        List<Pair<String,String>> switchOverModifierProtocols =
                singletonList( Pair.of( ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    public void shouldNotIncludeModifierProtocolInSwitchOverRequestIfUnsupportedVersion()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        client.handle( new ModifierProtocolResponse(
                StatusCode.SUCCESS, ModifierProtocolCategory.GRATUITOUS_OBFUSCATION.canonicalName(), "Rearrange the bytes at random" ) );

        // then
        List<Pair<String,String>> switchOverModifierProtocols =
                singletonList( Pair.of( ModifierProtocolCategory.COMPRESSION.canonicalName(), SNAPPY.implementation() ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( client.protocol().isDone() );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenSwitchOverResponseNotSuccess()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new SwitchOverResponse( StatusCode.FAILURE ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenProtocolStackNotSet()
    {
        // given
        client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new SwitchOverResponse( StatusCode.SUCCESS ) );

        // then
        assertCompletedExceptionally( client.protocol() );
    }

    @Test
    public void shouldCompleteProtocolStackOnSwitchoverResponse()
    {
        // given
        ModifierProtocolRepository repo = new ModifierProtocolRepository(
                TestModifierProtocols.values(),
                singletonList( new ModifierSupportedProtocols( ModifierProtocolCategory.COMPRESSION, emptyList() ) ) );

        client.initiate( channel, applicationProtocolRepository, repo );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );
        client.handle(
                new ModifierProtocolResponse( StatusCode.SUCCESS, SNAPPY.category(), SNAPPY.implementation() ) );

        // when
        client.handle( new SwitchOverResponse( StatusCode.SUCCESS ) );

        // then
        ProtocolStack protocolStack = client.protocol().getNow( null );
        assertThat( protocolStack, equalTo( new ProtocolStack( expectedApplicationProtocol, singletonList( SNAPPY ) ) ) );
    }

    private void assertCompletedExceptionally( CompletableFuture<ProtocolStack> protocolStackCompletableFuture )
    {
        assertTrue( protocolStackCompletableFuture.isCompletedExceptionally() );
        try
        {
            protocolStackCompletableFuture.getNow( null );
        }
        catch ( CompletionException ex )
        {
            assertThat( ex.getCause(), instanceOf( ClientHandshakeException.class ) );
        }
    }
}
