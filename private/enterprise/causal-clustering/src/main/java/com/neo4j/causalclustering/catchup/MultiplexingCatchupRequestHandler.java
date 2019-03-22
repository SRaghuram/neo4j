/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Function;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;

/**
 * Multiplexes catchup requests against different databases.
 *
 * @param <T> The type of request to multiplex.
 */
class MultiplexingCatchupRequestHandler<T extends CatchupProtocolMessage> extends SimpleChannelInboundHandler<T>
{
    private final Class<T> messageType;
    private final DatabaseManager<?> databaseManager;
    private final Function<Database,SimpleChannelInboundHandler<T>> handlerFactory;
    private final CatchupServerProtocol protocol;
    private final LogProvider logProvider;

    MultiplexingCatchupRequestHandler( CatchupServerProtocol protocol, DatabaseManager<?> databaseManager,
            Function<Database,SimpleChannelInboundHandler<T>> handlerFactory, Class<T> messageType, LogProvider logProvider )
    {
        super( messageType );
        this.messageType = messageType;
        this.databaseManager = databaseManager;
        this.handlerFactory = handlerFactory;
        this.protocol = protocol;
        this.logProvider = logProvider;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, T request ) throws Exception
    {
        String databaseName = request.databaseName();

        SimpleChannelInboundHandler<T> handler = databaseManager
                .getDatabaseContext( new DatabaseId( databaseName ) )
                .map( DatabaseContext::database )
                .map( handlerFactory )
                .orElseGet( () -> new UnknownDatabaseHandler<>( messageType, protocol, logProvider ) );

        handler.channelRead( ctx, request );
    }
}

