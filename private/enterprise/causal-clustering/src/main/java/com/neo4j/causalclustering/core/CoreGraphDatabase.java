/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.ClusteringEditionModule;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.impl.factory.DbmsInfo;

public class CoreGraphDatabase
{
    private final DatabaseManagementService managementService;

    public interface CoreEditionModuleFactory
    {
        ClusteringEditionModule create( GlobalModule globalModule, DiscoveryServiceFactory discoveryServiceFactory );
    }

    public CoreGraphDatabase( Config config, ExternalDependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory,
            CoreEditionModuleFactory editionModuleFactory )
    {
        managementService = createManagementService( config, dependencies, discoveryServiceFactory, editionModuleFactory );
    }

    protected DatabaseManagementService createManagementService( Config config, ExternalDependencies dependencies,
            DiscoveryServiceFactory discoveryServiceFactory, CoreEditionModuleFactory editionModuleFactory )
    {
        return new DatabaseManagementServiceFactory( DbmsInfo.CORE,
                globalModule -> editionModuleFactory.create( globalModule, discoveryServiceFactory ) )
                .build( config, dependencies );
    }

    public DatabaseManagementService getManagementService()
    {
        return managementService;
    }
}
