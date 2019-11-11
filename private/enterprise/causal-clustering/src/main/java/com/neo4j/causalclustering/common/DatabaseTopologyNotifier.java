/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.discovery.TopologyService;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class DatabaseTopologyNotifier extends LifecycleAdapter
{
    private final DatabaseId databaseId;
    private final TopologyService topologyService;

    public DatabaseTopologyNotifier( DatabaseId databaseId, TopologyService topologyService )
    {
        this.databaseId = databaseId;
        this.topologyService = topologyService;
    }

    @Override
    public void start()
    {
        topologyService.onDatabaseStart( databaseId );
    }

    @Override
    public void stop()
    {
        topologyService.onDatabaseStop( databaseId );
    }
}
