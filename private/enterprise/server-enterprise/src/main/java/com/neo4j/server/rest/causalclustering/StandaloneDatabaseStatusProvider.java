/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.discovery.TopologyService;

import java.util.Set;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.TransactionIdStore;

class StandaloneDatabaseStatusProvider
{
    private final Health databaseHealth;
    private final TopologyService topologyService;

    StandaloneDatabaseStatusProvider( GraphDatabaseAPI db )
    {
        var resolver = db.getDependencyResolver();
        this.databaseHealth = resolver.resolveDependency( DatabaseHealth.class );
        this.topologyService = resolver.resolveDependency( TopologyService.class );
    }

    ClusteringDatabaseStatusResponse currentStatus()
    {
        var myId = topologyService.serverId().uuid();
        return new ClusteringDatabaseStatusResponse( 0, false, Set.of(), databaseHealth.isHealthy(), myId,
                myId, null, null, false, topologyService.isHealthy() );
    }
}
