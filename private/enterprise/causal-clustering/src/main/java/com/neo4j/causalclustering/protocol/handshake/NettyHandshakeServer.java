/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class NettyHandshakeServer extends SimpleChannelInboundHandler<ServerMessage>
{
    private final HandshakeServer handler;

    public NettyHandshakeServer( HandshakeServer handler )
    {
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, ServerMessage msg )
    {
        msg.dispatch( handler );
    }
}
