/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.directory;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Sent from this Neo4J instance into discovery service
 */
public class LeaderInfoSettingMessage
{
    private final LeaderInfo leaderInfo;
    private final DatabaseId databaseId;

    public LeaderInfoSettingMessage( LeaderInfo leaderInfo, NamedDatabaseId namedDatabaseId )
    {
        this.leaderInfo = leaderInfo;
        this.databaseId = namedDatabaseId.databaseId();
    }

    public LeaderInfo leaderInfo()
    {
        return leaderInfo;
    }

    public DatabaseId database()
    {
        return databaseId;
    }
}
