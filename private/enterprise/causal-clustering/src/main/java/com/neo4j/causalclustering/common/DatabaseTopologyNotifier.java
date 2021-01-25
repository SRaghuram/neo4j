/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.discovery.TopologyService;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class DatabaseTopologyNotifier extends LifecycleAdapter
{
    private final NamedDatabaseId namedDatabaseId;
    private final TopologyService topologyService;

    public DatabaseTopologyNotifier( NamedDatabaseId namedDatabaseId, TopologyService topologyService )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.topologyService = topologyService;
    }

    @Override
    public void start()
    {
        topologyService.onDatabaseStart( namedDatabaseId );
    }

    @Override
    public void stop()
    {
        topologyService.onDatabaseStop( namedDatabaseId );
    }
}
