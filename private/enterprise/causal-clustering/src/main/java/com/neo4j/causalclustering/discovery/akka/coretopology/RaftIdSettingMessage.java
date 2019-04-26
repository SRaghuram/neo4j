/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import com.neo4j.causalclustering.identity.RaftId;

import org.neo4j.kernel.database.DatabaseId;

/**
 * Sent from this Neo4J instance into discovery service
 */
public class RaftIdSettingMessage
{
    private final RaftId raftId;
    private final DatabaseId database;

    public RaftIdSettingMessage( RaftId raftId, DatabaseId databaseId )
    {
        this.raftId = raftId;
        this.database = databaseId;
    }

    public RaftId raftId()
    {
        return raftId;
    }

    public DatabaseId database()
    {
        return database;
    }
}
