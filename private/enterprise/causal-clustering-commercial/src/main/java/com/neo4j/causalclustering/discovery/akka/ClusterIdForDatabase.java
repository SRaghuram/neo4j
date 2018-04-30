/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import org.neo4j.causalclustering.identity.ClusterId;

public class ClusterIdForDatabase
{
    private final ClusterId clusterId;
    private final String database;

    public ClusterIdForDatabase( ClusterId clusterId, String database )
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
