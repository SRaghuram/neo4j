/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.net.LoadBalancedTrackingChannelPoolMap.RaftGroupSocket;
import com.neo4j.causalclustering.net.PooledChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class RaftSenderTest
{
    @Test
    void shouldLogErrorOnNonBlockingIncludingAddressAndChannelId()
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        var channelPoolService = mock( RaftChannelPoolService.class );
        var failingWrite = new IllegalStateException( "Failing write" );

        EmbeddedChannel embeddedChannel = new EmbeddedChannel( new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                throw failingWrite;
            }
        } );
        var pooledChannel = mock( PooledChannel.class );
        when( pooledChannel.channel() ).thenReturn( embeddedChannel );
        when( channelPoolService.acquire( any( RaftGroupSocket.class ) ) ).thenReturn( completedFuture( pooledChannel ) );

        RaftSender raftSender = new RaftSender( logProvider, channelPoolService, channelPoolService );

        // when
        var socketAddress = new SocketAddress( 1 );
        raftSender.send( socketAddress,
                         RaftMessages.OutboundRaftMessageContainer.of( null, new RaftMessages.Timeout.Election( IdFactory.randomRaftMemberId() ) ),
                         false );

        assertThat( logProvider ).forClass( RaftSender.class ).forLevel( WARN )
                .assertExceptionForLogMessage( format( "Raft sender failed exceptionally [Address: %s]", socketAddress ) )
                .hasMessage( format( "[ChannelId: %s]", embeddedChannel.id() ) )
                .hasCause( failingWrite );
    }

    @ParameterizedTest
    @MethodSource( "dataMessages" )
    void shouldUseDataChannel( RaftMessages.RaftMessage raftMessage )
    {
        // Given the message is of type that should use data channel
        var dataChannel = mockRaftChannelPoolService();
        var controlChannel = mockRaftChannelPoolService();
        var raftSender = new RaftSender( mock( LogProvider.class ), dataChannel, controlChannel );

        var raftGroupId = new RaftGroupId( UUID.randomUUID() );
        var message = RaftMessages.OutboundRaftMessageContainer.of( raftGroupId, raftMessage );
        var to = mock( SocketAddress.class );

        // When message is sent
        raftSender.send( to, message );

        // Then it should be sent over data channel
        verify( dataChannel, atLeastOnce() ).acquire( any() );
        verify( controlChannel, never() ).acquire( any() );
    }

    @ParameterizedTest
    @MethodSource( "controlMessages" )
    void shouldUseControlChannel( RaftMessages.RaftMessage raftMessage )
    {
        // Given the message is of type that should use control channel
        var dataChannel = mockRaftChannelPoolService();
        var controlChannel = mockRaftChannelPoolService();
        var raftSender = new RaftSender( mock( LogProvider.class ), dataChannel, controlChannel );

        var raftGroupId = new RaftGroupId( UUID.randomUUID() );
        var message = RaftMessages.OutboundRaftMessageContainer.of( raftGroupId, raftMessage );
        var to = mock( SocketAddress.class );

        // When message is sent
        raftSender.send( to, message );

        // Then it should be sent over control channel
        verify( dataChannel, never() ).acquire( any() );
        verify( controlChannel, atLeastOnce() ).acquire( any() );
    }

    private static Stream<RaftMessages.RaftMessage> dataMessages()
    {
        var raftMemberId = new RaftMemberId( UUID.randomUUID() );
        return Stream.of(
                new RaftMessages.AppendEntries.Request( raftMemberId, 0, 0, 0, RaftLogEntry.empty, 0 ),
                new RaftMessages.NewEntry.Request( raftMemberId, null ),
                new RaftMessages.NewEntry.BatchRequest( null )
        );
    }

    private static Stream<RaftMessages.RaftMessage> controlMessages()
    {
        var raftMemberId = new RaftMemberId( UUID.randomUUID() );
        return Stream.of(
                new RaftMessages.Vote.Request( raftMemberId, 0, raftMemberId, 0, 0 ),
                new RaftMessages.Vote.Response( raftMemberId, 0, true ),
                new RaftMessages.Heartbeat( raftMemberId, 0, 0, 0 )
        );
    }

    private RaftChannelPoolService mockRaftChannelPoolService()
    {
        var pooledChannel = mock( PooledChannel.class, RETURNS_MOCKS );
        var completableFuture = CompletableFuture.completedFuture( pooledChannel );

        var poolService = mock( RaftChannelPoolService.class );
        when( poolService.acquire( any() ) ).thenReturn( completableFuture );
        return poolService;
    }
}
