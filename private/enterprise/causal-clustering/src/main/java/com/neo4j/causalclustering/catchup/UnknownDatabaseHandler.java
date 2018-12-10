/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import com.neo4j.causalclustering.messaging.DatabaseCatchupRequest;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class UnknownDatabaseHandler<T extends DatabaseCatchupRequest> extends SimpleChannelInboundHandler<T>
{
    private final CatchupServerProtocol protocol;
    private final Log log;

    UnknownDatabaseHandler( Class<T> messageType, CatchupServerProtocol protocol, LogProvider logProvider )
    {
        super( messageType );
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, T request )
    {
        CatchupErrorResponse errorResponse = newErrorResponse( request );
        log.warn( errorResponse.message() );
        ctx.write( ResponseMessageType.ERROR );
        ctx.writeAndFlush( errorResponse );
        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
    }

    private CatchupErrorResponse newErrorResponse( T request )
    {
        return new CatchupErrorResponse( CatchupResult.E_DATABASE_UNKNOWN,
                String.format( "CatchupRequest %s refused as intended database %s does not exist on this machine.", request, request.databaseName() ) );
    }
}
