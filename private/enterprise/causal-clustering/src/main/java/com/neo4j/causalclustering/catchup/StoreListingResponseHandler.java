/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class StoreListingResponseHandler extends SimpleChannelInboundHandler<PrepareStoreCopyResponse>
{
    private final CatchupClientProtocol protocol;
    private final CatchupResponseHandler handler;

    public StoreListingResponseHandler( CatchupClientProtocol protocol,
            CatchupResponseHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final PrepareStoreCopyResponse msg ) throws Exception
    {
        handler.onStoreListingResponse( msg );
        protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
    }
}

