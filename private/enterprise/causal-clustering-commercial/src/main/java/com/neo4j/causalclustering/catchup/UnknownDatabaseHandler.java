/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchupErrorResponse;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.messaging.DatabaseCatchupRequest;

class UnknownDatabaseHandler<T extends DatabaseCatchupRequest> extends SimpleChannelInboundHandler<T>
{
    private final CatchupServerProtocol protocol;

    UnknownDatabaseHandler( Class<T> messageType, CatchupServerProtocol protocol )
    {
        super( messageType );
        this.protocol = protocol;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, T request )
    {
        ctx.write( ResponseMessageType.ERROR );
        ctx.writeAndFlush( newErrorResponse( request ) );
        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
    }

    private CatchupErrorResponse newErrorResponse( T request )
    {
        return new CatchupErrorResponse( CatchupResult.E_DATABASE_UNKNOWN,
                String.format( "CatchupRequest %s refused as intended database %s does not exist on this machine.", request, request.databaseName() ) );
    }
}
