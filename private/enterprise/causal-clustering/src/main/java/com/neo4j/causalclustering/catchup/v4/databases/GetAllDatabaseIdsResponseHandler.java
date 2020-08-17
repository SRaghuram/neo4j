/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class GetAllDatabaseIdsResponseHandler extends SimpleChannelInboundHandler<GetAllDatabaseIdsResponse>
{
    private final CatchupResponseHandler handler;
    private final CatchupClientProtocol protocol;

    public GetAllDatabaseIdsResponseHandler( CatchupResponseHandler handler, CatchupClientProtocol protocol )
    {
        this.handler = handler;
        this.protocol = protocol;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetAllDatabaseIdsResponse response ) throws Exception
    {
        handler.onGetAllDatabaseIdsResponse( response );
        protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
    }
}
