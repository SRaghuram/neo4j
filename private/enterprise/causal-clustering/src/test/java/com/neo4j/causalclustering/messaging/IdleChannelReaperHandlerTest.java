/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import org.junit.Test;

import java.net.InetSocketAddress;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IdleChannelReaperHandlerTest
{
    @Test
    public void shouldRemoveChannelViaCallback()
    {
        // given
        AdvertisedSocketAddress address = new AdvertisedSocketAddress( "localhost", 1984 );
        ReconnectingChannels channels = new ReconnectingChannels();
        channels.putIfAbsent( address, mock( ReconnectingChannel.class) );

        IdleChannelReaperHandler reaper = new IdleChannelReaperHandler( channels );

        final InetSocketAddress socketAddress = address.socketAddress();

        final Channel channel = mock( Channel.class );
        when( channel.remoteAddress() ).thenReturn( socketAddress );

        final ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        when( context.channel() ).thenReturn( channel );

        // when
        reaper.userEventTriggered( context, IdleStateEvent.ALL_IDLE_STATE_EVENT );

        // then
        assertNull( channels.get( address ) );
    }
}
