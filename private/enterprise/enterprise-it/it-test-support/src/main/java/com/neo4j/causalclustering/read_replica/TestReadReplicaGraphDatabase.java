/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.read_replica;

import com.neo4j.causalclustering.common.TestClusterDatabaseManagementServiceFactory;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.kernel.impl.factory.DbmsInfo;

public class TestReadReplicaGraphDatabase extends ReadReplicaGraphDatabase
{
    public TestReadReplicaGraphDatabase( Config config, ExternalDependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory,
            ReadReplicaEditionModuleFactory editionModuleFactory )
    {
        super( config, dependencies, discoveryServiceFactory, editionModuleFactory );
    }

    @Override
    protected DatabaseManagementService createManagementService( Config config, ExternalDependencies dependencies,
            DiscoveryServiceFactory discoveryServiceFactory, ReadReplicaEditionModuleFactory editionModuleFactory )
    {
        return new TestClusterDatabaseManagementServiceFactory( DbmsInfo.READ_REPLICA,
            globalModule -> editionModuleFactory.create( globalModule, discoveryServiceFactory ) )
            .build( config, dependencies );
    }
}
