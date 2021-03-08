/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import com.neo4j.causalclustering.common.Cluster;

import java.util.concurrent.ExecutionException;

public interface ClusterFactory
{
    Cluster createCluster( ClusterConfig clusterConfig );

    default Cluster start( ClusterConfig config ) throws ExecutionException, InterruptedException
    {
        return createCluster( config )
                .start();
    }
}
