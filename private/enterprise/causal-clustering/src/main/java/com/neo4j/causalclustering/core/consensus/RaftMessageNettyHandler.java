/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.messaging.Inbound;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

@ChannelHandler.Sharable
public class RaftMessageNettyHandler extends SimpleChannelInboundHandler<RaftMessages.InboundRaftMessageContainer<?>>
        implements Inbound<RaftMessages.InboundRaftMessageContainer<?>>
{
    private Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> actual;
    private Log log;

    public RaftMessageNettyHandler( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void registerHandler( Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> actual )
    {
        this.actual = actual;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext channelHandlerContext, RaftMessages.InboundRaftMessageContainer<?> incomingMessage )
    {
        try
        {
            actual.handle( incomingMessage );
        }
        catch ( Exception e )
        {
            log.error( format( "Failed to process message %s", incomingMessage ), e );
        }
    }
}
