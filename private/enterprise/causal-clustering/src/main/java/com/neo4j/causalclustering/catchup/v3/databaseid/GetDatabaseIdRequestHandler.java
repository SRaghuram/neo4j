/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseIdRepository;

public class GetDatabaseIdRequestHandler extends SimpleChannelInboundHandler<GetDatabaseIdRequest>
{
    private final CatchupServerProtocol protocol;
    private final DatabaseManager<?> databaseManager;
    private final DatabaseIdRepository databaseIdRepository;

    public GetDatabaseIdRequestHandler( DatabaseManager<?> databaseManager, CatchupServerProtocol protocol )
    {
        this.protocol = protocol;
        this.databaseManager = databaseManager;
        this.databaseIdRepository = databaseManager.databaseIdRepository();
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetDatabaseIdRequest msg )
    {
        var databaseName = msg.databaseName();
        var databaseIdOptional = databaseIdRepository.getByName( databaseName );
        var startedDB = databaseManager.getDatabaseContext( databaseName ).map( dbContext -> dbContext.database().isStarted() )
                                       .orElse( false );

        if ( databaseIdOptional.isPresent() )
        {
            if ( !startedDB )
            {
                ctx.write( ResponseMessageType.ERROR );
                ctx.writeAndFlush( stoppedDatabaseResponse( databaseName ) );
            }
            else
            {
                ctx.write( ResponseMessageType.DATABASE_ID_RESPONSE );
                ctx.writeAndFlush( databaseIdOptional.get().databaseId() );
            }
        }
        else
        {
            ctx.write( ResponseMessageType.ERROR );
            ctx.writeAndFlush( unknownDatabaseResponse( databaseName ) );
        }

        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
    }

    private static CatchupErrorResponse unknownDatabaseResponse( String databaseName )
    {
        return new CatchupErrorResponse( CatchupResult.E_DATABASE_UNKNOWN, "Database '" + databaseName + "' does not exist" );
    }

    private static CatchupErrorResponse stoppedDatabaseResponse( String databaseName )
    {
        return new CatchupErrorResponse( CatchupResult.E_STORE_UNAVAILABLE, "Database '" + databaseName + "' is stopped. " +
                                                                           "Start the database before backup" );
    }
}
