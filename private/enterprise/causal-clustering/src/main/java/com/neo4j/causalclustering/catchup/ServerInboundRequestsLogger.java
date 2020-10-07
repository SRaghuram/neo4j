/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ServerInboundRequestsLogger extends SimpleChannelInboundHandler<Object>
{
    private final Log log;

    public ServerInboundRequestsLogger( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, Object msg )
    {
        if ( msg instanceof CatchupProtocolMessage )
        {
            log.info( "Handling %s [From: %s]", ((CatchupProtocolMessage) msg).describe(), ctx.channel().remoteAddress() );
        }
        else
        {
            log.info( "Handling unexpected message type '%s' with message '%s' from [%s]", msg.getClass().getSimpleName(), msg, ctx.channel().remoteAddress() );
        }
        ctx.fireChannelRead( msg );
    }
}