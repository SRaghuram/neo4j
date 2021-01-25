/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.info;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class InfoResponseHandler extends SimpleChannelInboundHandler<InfoResponse>
{
    private final CatchupResponseHandler handler;
    private final CatchupClientProtocol protocol;

    public InfoResponseHandler( CatchupResponseHandler handler, CatchupClientProtocol protocol )
    {
        this.handler = handler;
        this.protocol = protocol;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, InfoResponse msg ) throws Exception
    {
        handler.onInfo( msg );
        protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
    }
}
