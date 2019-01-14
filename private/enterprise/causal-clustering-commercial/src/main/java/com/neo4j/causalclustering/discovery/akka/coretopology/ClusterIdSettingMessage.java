/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import org.neo4j.causalclustering.identity.ClusterId;

/**
 * Sent from this Neo4J instance into discovery service
 */
public class ClusterIdSettingMessage
{
    private final ClusterId clusterId;
    private final String database;

    public ClusterIdSettingMessage( ClusterId clusterId, String database )
    {
        this.clusterId = clusterId;
        this.database = database;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public String database()
    {
        return database;
    }
}
