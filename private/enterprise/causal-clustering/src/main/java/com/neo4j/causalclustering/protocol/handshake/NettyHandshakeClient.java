/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class NettyHandshakeClient extends SimpleChannelInboundHandler<ClientMessage>
{
    private final HandshakeClient handler;

    public NettyHandshakeClient( HandshakeClient handler )
    {
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, ClientMessage msg )
    {
        msg.dispatch( handler );
    }
}
