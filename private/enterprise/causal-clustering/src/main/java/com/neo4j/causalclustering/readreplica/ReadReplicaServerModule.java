/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.CatchupServersModule;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.common.PipelineBuilders;
import com.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;

import java.util.Optional;

import org.neo4j.graphdb.factory.module.GlobalModule;

public class ReadReplicaServerModule extends CatchupServersModule
{
    ReadReplicaServerModule( ClusteredDatabaseManager<ReadReplicaDatabaseContext> clusteredDatabaseManager, PipelineBuilders pipelineBuilders,
            CatchupHandlerFactory handlerFactory, GlobalModule globalModule, String databaseName )
    {
        super( clusteredDatabaseManager, pipelineBuilders, globalModule, databaseName );

        CatchupServerHandler catchupServerHandler = handlerFactory.create( null );
        InstalledProtocolHandler installedProtocolsHandler = new InstalledProtocolHandler();
        catchupServer = createCatchupServer( installedProtocolsHandler, catchupServerHandler, databaseName );
        backupServer = createBackupServer( installedProtocolsHandler, catchupServerHandler, databaseName );
    }

    @Override
    public Server catchupServer()
    {
        return catchupServer;
    }

    @Override
    public Optional<Server> backupServer()
    {
        return backupServer;
    }
}

