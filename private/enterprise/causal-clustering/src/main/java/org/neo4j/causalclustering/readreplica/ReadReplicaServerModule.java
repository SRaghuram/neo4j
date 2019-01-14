/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import java.util.Optional;

import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.catchup.CatchupServersModule;
import org.neo4j.causalclustering.common.PipelineBuilders;
import org.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import org.neo4j.causalclustering.net.InstalledProtocolHandler;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.graphdb.factory.module.PlatformModule;

public class ReadReplicaServerModule extends CatchupServersModule
{
    ReadReplicaServerModule( DatabaseService databaseService, PipelineBuilders pipelineBuilders, CatchupHandlerFactory handlerFactory,
            PlatformModule platformModule, String activeDatabaseName )
    {
        super( databaseService, pipelineBuilders, platformModule );

        CatchupServerHandler catchupServerHandler = handlerFactory.create( null );
        InstalledProtocolHandler installedProtocolsHandler = new InstalledProtocolHandler();
        catchupServer = createCatchupServer( installedProtocolsHandler, catchupServerHandler, activeDatabaseName );
        backupServer = createBackupServer( installedProtocolsHandler, catchupServerHandler, activeDatabaseName );
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

