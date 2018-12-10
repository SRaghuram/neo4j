/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;

import java.net.InetSocketAddress;

import org.neo4j.helpers.AdvertisedSocketAddress;

public class IdleChannelReaperHandler extends ChannelDuplexHandler
{
    private ReconnectingChannels channels;

    public IdleChannelReaperHandler( ReconnectingChannels channels )
    {
        this.channels = channels;
    }

    @Override
    public void userEventTriggered( ChannelHandlerContext ctx, Object evt )
    {
        if ( evt instanceof IdleStateEvent && evt == IdleStateEvent.ALL_IDLE_STATE_EVENT )
        {
            final InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            final AdvertisedSocketAddress address =
                    new AdvertisedSocketAddress( socketAddress.getHostName(), socketAddress.getPort() );

            channels.remove( address );
        }
    }
}
