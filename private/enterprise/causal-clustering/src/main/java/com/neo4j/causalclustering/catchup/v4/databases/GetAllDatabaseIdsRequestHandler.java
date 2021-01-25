/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseIdFactory;

public class GetAllDatabaseIdsRequestHandler extends SimpleChannelInboundHandler<GetAllDatabaseIdsRequest>
{
    private final CatchupServerProtocol protocol;
    private final DatabaseManager<?> databaseManager;

    public GetAllDatabaseIdsRequestHandler( CatchupServerProtocol protocol, DatabaseManager<?> databaseManager )
    {
        this.protocol = protocol;
        this.databaseManager = databaseManager;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetAllDatabaseIdsRequest request ) throws Exception
    {
        final var databaseIds = databaseManager.registeredDatabases().keySet()
                                               .stream()
                                               .map( d -> DatabaseIdFactory.from( d.name(), d.databaseId().uuid() ) )
                                               .collect( Collectors.toSet() );

        ctx.writeAndFlush( ResponseMessageType.ALL_DATABASE_IDS_RESPONSE );
        ctx.writeAndFlush( new GetAllDatabaseIdsResponse( databaseIds ) );
        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
    }
}
