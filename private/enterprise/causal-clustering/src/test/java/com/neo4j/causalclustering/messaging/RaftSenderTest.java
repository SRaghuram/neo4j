/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.net.LoadBalancedTrackingChannelPoolMap.RaftGroupSocket;
import com.neo4j.causalclustering.net.PooledChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.AssertableLogProvider;

import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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

        RaftSender raftSender = new RaftSender( logProvider, channelPoolService );

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
}
