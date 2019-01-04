/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.causalclustering.catchup.CatchupErrorResponse;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.messaging.DatabaseCatchupRequest;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Multiplexes catchup requests against different databases.
 *
 * @param <T> The type of request to multiplex.
 */
public class MultiplexingCatchupRequestHandler<T extends DatabaseCatchupRequest> extends SimpleChannelInboundHandler<T>
{
    private final Function<LocalDatabase,SimpleChannelInboundHandler<T>> handlerFactory;
    private final DatabaseService databaseService;
    private final Map<String,SimpleChannelInboundHandler<T>> wrappedHandlers;
    private final CatchupServerProtocol protocol;
    private final UnknownDatabaseHandler UNKNOWN_DATABASE_HANDLER = new UnknownDatabaseHandler();
    private final Log log;

    MultiplexingCatchupRequestHandler( CatchupServerProtocol protocol, Function<LocalDatabase,SimpleChannelInboundHandler<T>> handlerFactory,
            Class<T> handlerTypeHint, DatabaseService databaseService, LogProvider logProvider )
    {
        super( handlerTypeHint );
        this.handlerFactory = handlerFactory;
        this.databaseService = databaseService;
        this.wrappedHandlers = new HashMap<>();
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, T request ) throws Exception
    {
        String databaseName = request.databaseName();

        SimpleChannelInboundHandler<T> handler = databaseService.get( databaseName )
                .map( localDatabase -> wrappedHandlers.computeIfAbsent( databaseName, ignored -> handlerFactory.apply( localDatabase ) ) )
                .orElse( UNKNOWN_DATABASE_HANDLER );

        handler.channelRead( ctx, request );
    }

    private class UnknownDatabaseHandler extends SimpleChannelInboundHandler<T>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext ctx, T msg )
        {
            String message = String.format( "CatchupRequest %s refused as intended database %s does not exist on this machine.", msg, msg.databaseName() );
            log.warn( message );
            ctx.write( ResponseMessageType.ERROR );
            ctx.writeAndFlush( new CatchupErrorResponse( CatchupResult.E_DATABASE_UNKNOWN,
                    message ) );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
    }
}

