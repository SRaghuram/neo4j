/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class CoreGraphDatabase extends GraphDatabaseFacade
{

    private final DatabaseManagementService managementService;

    public interface CoreEditionModuleFactory
    {
        AbstractCoreEditionModule create( GlobalModule globalModule, DiscoveryServiceFactory discoveryServiceFactory );
    }

    public CoreGraphDatabase( File storeDir, Config config,
            ExternalDependencies dependencies,
            DiscoveryServiceFactory discoveryServiceFactory,
            CoreEditionModuleFactory editionModuleFactory )
    {
        managementService = new DatabaseManagementServiceFactory( DatabaseInfo.CORE,
                globalModule -> editionModuleFactory.create( globalModule, discoveryServiceFactory ) )
                .build( storeDir, config, dependencies );
    }

    public DatabaseManagementService getManagementService()
    {
        return managementService;
    }
}
