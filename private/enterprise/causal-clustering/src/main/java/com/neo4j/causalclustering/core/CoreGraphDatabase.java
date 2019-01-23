/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;

import java.io.File;

import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class CoreGraphDatabase extends GraphDatabaseFacade
{
    public interface CoreEditionModuleFactory
    {
        AbstractCoreEditionModule create( GlobalModule globalModule, DiscoveryServiceFactory discoveryServiceFactory );
    }

    private AbstractCoreEditionModule editionModule;
    private final CoreEditionModuleFactory editionModuleFactory;

    public CoreGraphDatabase( File storeDir, Config config,
            ExternalDependencies dependencies,
            DiscoveryServiceFactory discoveryServiceFactory,
            CoreEditionModuleFactory editionModuleFactory )
    {
        this.editionModuleFactory = editionModuleFactory;
        new GraphDatabaseFacadeFactory( DatabaseInfo.CORE, globalModule -> cachingFactory( globalModule, discoveryServiceFactory ) )
                .initFacade( storeDir, config, dependencies, this );
    }

    public Role getRole()
    {
        return editionModule
                .consensusModule()
                .raftMachine()
                .currentRole();
    }

    private AbstractEditionModule cachingFactory( GlobalModule globalModule, DiscoveryServiceFactory discoveryServiceFactory )
    {
        if ( editionModule == null )
        {
            editionModule = editionModuleFactory.create( globalModule, discoveryServiceFactory );
        }
        return editionModule;
    }

    public void disableCatchupServer() throws Throwable
    {
        editionModule.disableCatchupServer();
    }
}
