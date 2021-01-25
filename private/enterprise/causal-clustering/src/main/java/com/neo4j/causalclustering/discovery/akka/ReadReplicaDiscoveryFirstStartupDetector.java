/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.DiscoveryFirstStartupDetector;

public class ReadReplicaDiscoveryFirstStartupDetector implements DiscoveryFirstStartupDetector
{
    /**
     * Read replicas cannot bootstrap so there's no benefit to skipping cluster detection. So we always return false.
     * @return false
     */
    @Override
    public Boolean isFirstStartup()
    {
        return false;
    }
}
