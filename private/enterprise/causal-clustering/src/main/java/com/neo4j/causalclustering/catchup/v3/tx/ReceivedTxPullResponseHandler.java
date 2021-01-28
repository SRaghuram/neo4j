/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import com.neo4j.causalclustering.catchup.tx.ReceivedTxPullResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ReceivedTxPullResponseHandler extends SimpleChannelInboundHandler<ReceivedTxPullResponse>
{
    private final CatchupClientProtocol protocol;
    private final CatchupResponseHandler handler;

    public ReceivedTxPullResponseHandler( CatchupClientProtocol protocol, CatchupResponseHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final ReceivedTxPullResponse msg )
    {
        if ( protocol.isExpecting( CatchupClientProtocol.State.TX_PULL_RESPONSE ) )
        {
            if ( msg.equals( ReceivedTxPullResponse.EMPTY ) )
            {
                protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
            }
            else
            {
                handler.onTxPullResponse( msg );
            }
        }
        else
        {
            ctx.fireChannelRead( msg );
        }
    }
}
