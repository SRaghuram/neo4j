/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.messaging.DatabaseCatchupRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.logging.LogProvider;

/**
 * Multiplexes catchup requests against different databases.
 *
 * @param <T> The type of request to multiplex.
 */
class MultiplexingCatchupRequestHandler<T extends DatabaseCatchupRequest> extends SimpleChannelInboundHandler<T>
{
    private final Class<T> messageType;
    private final Supplier<DatabaseManager> databaseManagerSupplier;
    private final Function<Database,SimpleChannelInboundHandler<T>> handlerFactory;
    private final CatchupServerProtocol protocol;
    private final LogProvider logProvider;

    MultiplexingCatchupRequestHandler( CatchupServerProtocol protocol, Supplier<DatabaseManager> databaseManagerSupplier,
            Function<Database,SimpleChannelInboundHandler<T>> handlerFactory, Class<T> messageType, LogProvider logProvider )
    {
        super( messageType );
        this.messageType = messageType;
        this.databaseManagerSupplier = databaseManagerSupplier;
        this.handlerFactory = handlerFactory;
        this.protocol = protocol;
        this.logProvider = logProvider;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, T request ) throws Exception
    {
        String databaseName = request.databaseName();

        SimpleChannelInboundHandler<T> handler = databaseManagerSupplier.get()
                .getDatabaseContext( databaseName )
                .map( DatabaseContext::getDatabase )
                .map( handlerFactory )
                .orElseGet( () -> new UnknownDatabaseHandler<>( messageType, protocol, logProvider ) );

        handler.channelRead( ctx, request );
    }
}

