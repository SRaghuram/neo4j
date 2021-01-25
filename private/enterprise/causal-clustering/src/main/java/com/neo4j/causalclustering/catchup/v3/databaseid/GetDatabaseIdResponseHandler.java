/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class GetDatabaseIdResponseHandler extends SimpleChannelInboundHandler<GetDatabaseIdResponse>
{
    private final CatchupResponseHandler handler;
    private final CatchupClientProtocol protocol;

    public GetDatabaseIdResponseHandler( CatchupClientProtocol protocol, CatchupResponseHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final GetDatabaseIdResponse msg )
    {
        handler.onGetDatabaseIdResponse( msg );
        protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
    }
}
