/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import java.io.File;
import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class CoreGraphDatabase extends GraphDatabaseFacade
{
    private EnterpriseCoreEditionModule editionModule;

    protected CoreGraphDatabase()
    {
    }

    public CoreGraphDatabase( File storeDir, Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory )
    {
        Function<PlatformModule,AbstractEditionModule> factory = platformModule ->
        {
            editionModule = new EnterpriseCoreEditionModule( platformModule, discoveryServiceFactory );
            return editionModule;
        };
        new GraphDatabaseFacadeFactory( DatabaseInfo.CORE, factory ).initFacade( storeDir, config, dependencies, this );
    }

    public Role getRole()
    {
        return getDependencyResolver().resolveDependency( RaftMachine.class ).currentRole();
    }

    public void disableCatchupServer() throws Throwable
    {
        editionModule.disableCatchupServer();
    }
}
