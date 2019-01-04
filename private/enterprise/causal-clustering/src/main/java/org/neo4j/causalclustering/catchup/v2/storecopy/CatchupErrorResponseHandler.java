/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2.storecopy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchupClientProtocol;
import org.neo4j.causalclustering.catchup.CatchupErrorResponse;
import org.neo4j.causalclustering.catchup.CatchupResponseHandler;

public class CatchupErrorResponseHandler extends SimpleChannelInboundHandler<CatchupErrorResponse>
{
    private final CatchupClientProtocol protocol;
    private final CatchupResponseHandler handler;

    public CatchupErrorResponseHandler( CatchupClientProtocol protocol, CatchupResponseHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final CatchupErrorResponse msg )
    {
        handler.onCatchupErrorResponse( msg );
        protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
    }
}
