/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.metadata;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class GetMetadataRequestHandler extends SimpleChannelInboundHandler<GetMetadataRequest>
{
    private final CatchupServerProtocol protocol;
    private final DependencyResolver dependencyResolver;
    private final DatabaseManager<?> databaseManager;

    public GetMetadataRequestHandler( CatchupServerProtocol protocol, DependencyResolver dependencyResolver,
                                      DatabaseManager<?> databaseManager )
    {
        this.protocol = protocol;
        this.dependencyResolver = dependencyResolver;
        this.databaseManager = databaseManager;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetMetadataRequest request ) throws Exception
    {
        ctx.writeAndFlush( ResponseMessageType.METADATA_RESPONSE );
        //will be replaced when cypher team implement the API
        final var backupCommands = getBackupCommands( request );
        ctx.writeAndFlush( new GetMetadataResponse( backupCommands.getCommands() ) );

        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
    }

    private DatabaseSecurityCommands getBackupCommands( GetMetadataRequest request )
    {
        final var backupCommandProvider = dependencyResolver.resolveDependency( DatabaseSecurityCommandsProvider.class );
        return databaseManager.getDatabaseContext( SYSTEM_DATABASE_NAME )
                              .map( DatabaseContext::databaseFacade )
                              .map( facade ->
                                    {
                                        try ( Transaction tx = facade.beginTx() )
                                        {
                                            final var addUsers = request.includeMetadata.addUsers;
                                            final var addRoles = request.includeMetadata.addRoles;

                                            return backupCommandProvider.getBackupCommands( tx, request.databaseName, addUsers, addRoles );
                                        }
                                    } )
                              .orElse( new DatabaseSecurityCommands( List.of(), List.of() ) );
    }
}
