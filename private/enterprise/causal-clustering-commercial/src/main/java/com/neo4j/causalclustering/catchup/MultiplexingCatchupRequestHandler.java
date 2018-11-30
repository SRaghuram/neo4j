/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.messaging.DatabaseCatchupRequest;

/**
 * Multiplexes catchup requests against different databases.
 *
 * @param <T> The type of request to multiplex.
 */
class MultiplexingCatchupRequestHandler<T extends DatabaseCatchupRequest> extends SimpleChannelInboundHandler<T>
{
    private final Class<T> messageType;
    private final Function<String,SimpleChannelInboundHandler<T>> handlerFactory;
    private final Map<String,SimpleChannelInboundHandler<T>> wrappedHandlers;
    private final CatchupServerProtocol protocol;

    MultiplexingCatchupRequestHandler( CatchupServerProtocol protocol, Function<String,SimpleChannelInboundHandler<T>> handlerFactory, Class<T> messageType )
    {
        super( messageType );
        this.messageType = messageType;
        this.handlerFactory = handlerFactory;
        this.wrappedHandlers = new HashMap<>();
        this.protocol = protocol;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, T request ) throws Exception
    {
        String databaseName = request.databaseName();

        SimpleChannelInboundHandler<T> handler = wrappedHandlers.computeIfAbsent( databaseName, handlerFactory );
        if ( handler == null )
        {
            handler = new UnknownDatabaseHandler<>( messageType, protocol );
        }

        handler.channelRead( ctx, request );
    }
}

