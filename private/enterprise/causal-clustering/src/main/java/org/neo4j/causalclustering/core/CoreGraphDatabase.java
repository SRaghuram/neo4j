/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import java.io.File;

import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.util.VisibleForTesting;

public class CoreGraphDatabase extends GraphDatabaseFacade
{
    protected EnterpriseCoreEditionModule editionModule;

    public CoreGraphDatabase( File storeDir, Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory )
    {
        new GraphDatabaseFacadeFactory( DatabaseInfo.CORE, platformModule -> cachingFactory( platformModule, discoveryServiceFactory ) )
                .initFacade( storeDir, config, dependencies, this );
    }

    //TODO: Refactor - the interaction between this and CommercialCoreGraphDatabase is gross
    protected CoreGraphDatabase()
    { //no-op
    }

    public Role getRole()
    {
        return editionModule
                .consensusModule()
                .raftMachine()
                .currentRole();
    }

    private EnterpriseCoreEditionModule cachingFactory( PlatformModule platformModule, DiscoveryServiceFactory discoveryServiceFactory )
    {
        if ( editionModule == null )
        {
            editionModule = new EnterpriseCoreEditionModule( platformModule, discoveryServiceFactory );
        }
        return editionModule;
    }

    @VisibleForTesting
    void disableCatchupServer() throws Throwable
    {
        editionModule.disableCatchupServer();
    }
}
