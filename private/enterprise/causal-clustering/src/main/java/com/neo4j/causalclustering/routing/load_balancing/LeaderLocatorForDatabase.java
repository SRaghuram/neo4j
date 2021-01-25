/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;

import java.util.Optional;

import org.neo4j.kernel.database.NamedDatabaseId;

public interface LeaderLocatorForDatabase
{
    Optional<LeaderLocator> getLeader( NamedDatabaseId namedDatabaseId );
}
