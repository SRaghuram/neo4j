/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.server.CatchupHandlerFactory;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.net.Server;

import java.util.Optional;

public class ReadReplicaServerModule
{
    private final Server catchupServer;
    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private final Optional<Server> backupServer;
    private final CatchupComponentsRepository catchupComponents;
    private final CatchupClientFactory catchupClientFactory;

    ReadReplicaServerModule( ClusteredDatabaseManager<ReadReplicaDatabaseContext> databaseManager, CatchupComponentsProvider catchupComponentsProvider,
            CatchupHandlerFactory handlerFactory, String databaseName )
    {
        CatchupServerHandler catchupServerHandler = handlerFactory.create( null );
        InstalledProtocolHandler installedProtocolsHandler = new InstalledProtocolHandler();
        this.catchupServer = catchupComponentsProvider.createCatchupServer( installedProtocolsHandler, catchupServerHandler, databaseName );
        this.backupServer = catchupComponentsProvider.createBackupServer( installedProtocolsHandler, catchupServerHandler, databaseName );
        this.catchupComponents = new CatchupComponentsRepository( databaseManager );
        this.catchupClientFactory = catchupComponentsProvider.createCatchupClient();
    }

    public Server catchupServer()
    {
        return catchupServer;
    }

    Optional<Server> backupServer()
    {
        return backupServer;
    }

    CatchupComponentsRepository catchupComponents()
    {
        return catchupComponents;
    }

    CatchupClientFactory catchupClient()
    {
        return catchupClientFactory;
    }
}

