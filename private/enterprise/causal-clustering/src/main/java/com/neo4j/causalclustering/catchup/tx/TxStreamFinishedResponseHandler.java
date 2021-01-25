/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import static com.neo4j.causalclustering.catchup.CatchupClientProtocol.State;

public class TxStreamFinishedResponseHandler extends SimpleChannelInboundHandler<TxStreamFinishedResponse>
{
    private final CatchupClientProtocol protocol;
    private final CatchupResponseHandler handler;

    public TxStreamFinishedResponseHandler( CatchupClientProtocol protocol, CatchupResponseHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, TxStreamFinishedResponse msg )
    {
        handler.onTxStreamFinishedResponse( msg );
        protocol.expect( State.MESSAGE_TYPE );
    }
}
