/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import com.neo4j.causalclustering.identity.ClusterId;

import org.neo4j.kernel.database.DatabaseId;

/**
 * Sent from this Neo4J instance into discovery service
 */
public class ClusterIdSettingMessage
{
    private final ClusterId clusterId;
    private final DatabaseId database;

    public ClusterIdSettingMessage( ClusterId clusterId, DatabaseId databaseId )
    {
        this.clusterId = clusterId;
        this.database = databaseId;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public DatabaseId database()
    {
        return database;
    }
}
