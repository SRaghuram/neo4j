/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.error.UnavailableDatabaseHandler;
import com.neo4j.causalclustering.catchup.error.UnknownDatabaseHandler;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Function;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.logging.LogProvider;

/**
 * Multiplexes catchup requests against different databases.
 *
 * @param <T> The type of request to multiplex.
 */
class MultiplexingCatchupRequestHandler<T extends CatchupProtocolMessage.WithDatabaseId> extends SimpleChannelInboundHandler<T>
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
        var databaseId = request.databaseId();
        var databaseContext = databaseManager.getDatabaseContext( databaseId ).orElse( null );
        var handler = createHandler( databaseContext );
        handler.channelRead( ctx, request );
    }

    private SimpleChannelInboundHandler<T> createHandler( DatabaseContext databaseContext )
    {
        if ( databaseContext == null )
        {
            return new UnknownDatabaseHandler<>( messageType, protocol, logProvider );
        }

        var availabilityGuard = databaseContext.database().getDatabaseAvailabilityGuard();
        if ( !availabilityGuard.isAvailable() )
        {
            return new UnavailableDatabaseHandler<>( messageType, protocol, availabilityGuard, logProvider );
        }

        return handlerFactory.apply( databaseContext.database() );
    }
}

