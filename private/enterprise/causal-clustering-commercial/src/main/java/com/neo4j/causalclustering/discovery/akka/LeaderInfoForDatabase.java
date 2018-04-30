/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;

public class LeaderInfoForDatabase
{
    private final LeaderInfo leaderInfo;
    private final String databaseName;

    public LeaderInfoForDatabase( LeaderInfo leaderInfo, String databaseName )
    {
        this.leaderInfo = leaderInfo;
        this.databaseName = databaseName;
    }

    public LeaderInfo leaderInfo()
    {
        return leaderInfo;
    }

    public String database()
    {
        return databaseName;
    }
}
