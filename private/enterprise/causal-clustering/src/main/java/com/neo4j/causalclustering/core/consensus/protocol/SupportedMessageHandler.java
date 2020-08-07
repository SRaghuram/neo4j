/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.protocol;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.messaging.marshalling.SupportedMessages;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class SupportedMessageHandler extends MessageToMessageEncoder<RaftMessages.OutboundRaftMessageContainer<?>>
{
    private final RaftMessages.Handler<Boolean,?> isSupportedHandler;

    public SupportedMessageHandler( RaftMessages.Handler<Boolean,?> isSupportedHandler )
    {
        this.isSupportedHandler = isSupportedHandler;
    }

    @Override
    protected void encode( ChannelHandlerContext channelHandlerContext, RaftMessages.OutboundRaftMessageContainer<?> outbound, List<Object> list )
            throws Exception
    {
        var message = outbound.message();

        if ( message.dispatch( isSupportedHandler ) )
        {
            list.add( outbound );
        }
    }
}
