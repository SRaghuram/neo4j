/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;

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

