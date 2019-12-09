/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.net.PooledChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.AssertableLogProvider;

import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.test.assertion.Assert.assertEventually;

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
        when( channelPoolService.acquire( any( SocketAddress.class ) ) ).thenReturn( completedFuture( pooledChannel ) );

        RaftSender raftSender = new RaftSender( logProvider, channelPoolService );

        // when
        var socketAddress = new SocketAddress( 1 );
        raftSender.send( socketAddress, new Message()
        { }, false );

        // then
        assertEventually( () -> logProvider.containsMatchingLogCall( AssertableLogProvider
                        .inLog( RaftSender.class )
                        .warn( Matchers.equalTo( format( "Raft sender failed exceptionally [Address: %s]", socketAddress ) ), new BaseMatcher<>()
                        {
                            @Override
                            public void describeTo( Description description )
                            {

                            }

                            @Override
                            public boolean matches( Object o )
                            {
                                CompletionException exception = (CompletionException) o;
                                return format( "[ChannelId: %s]", embeddedChannel.id() ).equals( exception.getMessage() ) &&
                                        exception.getCause().equals( failingWrite );
                            }
                        } ) ),
                Matchers.is( true ), 1,
                TimeUnit.MINUTES );
    }
}
