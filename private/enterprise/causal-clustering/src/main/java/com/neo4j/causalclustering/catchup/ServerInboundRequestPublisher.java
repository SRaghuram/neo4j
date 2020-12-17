/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ServerInboundRequestPublisher extends SimpleChannelInboundHandler<Object>
{
    private final CatchupInboundEventListener listener;

    public ServerInboundRequestPublisher( CatchupInboundEventListener listener )
    {
        this.listener = listener;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, Object msg )
    {
        var remoteAddress = ctx.channel().remoteAddress();
        if ( msg instanceof TxPullRequest )
        {
            listener.onTxPullRequest( remoteAddress, (TxPullRequest) msg );
        }
        else if ( msg instanceof CatchupProtocolMessage )
        {
            listener.onCatchupProtocolMessage( remoteAddress, (CatchupProtocolMessage) msg );
        }
        else
        {
            listener.onOtherMessage( remoteAddress, msg );
        }
        ctx.fireChannelRead( msg );
    }
}
