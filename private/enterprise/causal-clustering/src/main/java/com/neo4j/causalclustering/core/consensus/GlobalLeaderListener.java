/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import org.neo4j.kernel.database.NamedDatabaseId;

public interface GlobalLeaderListener
{
    void onLeaderSwitch( NamedDatabaseId databaseId, LeaderInfo leaderInfo );

    default void onUnregister( NamedDatabaseId forDatabase )
    {
        // do nothing
    }
}
