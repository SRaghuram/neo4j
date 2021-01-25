/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.error;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

abstract class ErrorReportingHandler<T extends CatchupProtocolMessage> extends SimpleChannelInboundHandler<T>
{
    private final CatchupServerProtocol protocol;
    private final Log log;

    ErrorReportingHandler( Class<T> messageType, CatchupServerProtocol protocol, LogProvider logProvider )
    {
        super( messageType );
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected final void channelRead0( ChannelHandlerContext ctx, T request )
    {
        CatchupErrorResponse errorResponse = newErrorResponse( request );
        log.warn( errorResponse.message() );
        ctx.write( ResponseMessageType.ERROR );
        ctx.writeAndFlush( errorResponse );
        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
    }

    abstract CatchupErrorResponse newErrorResponse( T request );
}
