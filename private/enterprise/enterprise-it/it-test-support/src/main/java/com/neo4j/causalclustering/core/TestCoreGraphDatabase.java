/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.TestClusterDatabaseManagementServiceFactory;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.kernel.impl.factory.DbmsInfo;

public class TestCoreGraphDatabase extends CoreGraphDatabase
{
    public TestCoreGraphDatabase( Config config, ExternalDependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory,
            CoreEditionModuleFactory editionModuleFactory )
    {
        super( config, dependencies, discoveryServiceFactory, editionModuleFactory );
    }

    @Override
    protected DatabaseManagementService createManagementService( Config config, ExternalDependencies dependencies,
            DiscoveryServiceFactory discoveryServiceFactory, CoreEditionModuleFactory editionFactory )
    {
        return new TestClusterDatabaseManagementServiceFactory( DbmsInfo.CORE,
                globalModule -> editionFactory.create( globalModule, discoveryServiceFactory ) )
                .build( config, dependencies );
    }
}
