/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class GetStoreIdResponseHandler extends SimpleChannelInboundHandler<GetStoreIdResponse>
{
    private final CatchupResponseHandler handler;
    private final CatchupClientProtocol protocol;

    public GetStoreIdResponseHandler( CatchupClientProtocol protocol, CatchupResponseHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final GetStoreIdResponse msg )
    {
        handler.onGetStoreIdResponse( msg );
        protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
    }
}
