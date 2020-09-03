/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;

import static java.nio.file.Files.notExists;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class DefaultDiscoveryFirstStartupDetector implements DiscoveryFirstStartupDetector
{
    private final ClusterStateLayout clusterStateLayout;

    public DefaultDiscoveryFirstStartupDetector( ClusterStateLayout clusterStateLayout )
    {
        this.clusterStateLayout = clusterStateLayout;
    }

    @Override
    public Boolean isFirstStartup()
    {
        // this how we figure out if core has ever formed a cluster before
        return notExists( clusterStateLayout.raftGroupIdFile( SYSTEM_DATABASE_NAME ) );
    }
}
