/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.tx;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchupResponseHandler;
import org.neo4j.causalclustering.catchup.CatchupClientProtocol;

public class TxPullResponseHandler extends SimpleChannelInboundHandler<TxPullResponse>
{
    private final CatchupClientProtocol protocol;
    private final CatchupResponseHandler handler;

    public TxPullResponseHandler( CatchupClientProtocol protocol,
                                  CatchupResponseHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final TxPullResponse msg )
    {
        if ( protocol.isExpecting( CatchupClientProtocol.State.TX_PULL_RESPONSE ) )
        {
            handler.onTxPullResponse( msg );
            protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
        }
        else
        {
            ctx.fireChannelRead( msg );
        }
    }
}
