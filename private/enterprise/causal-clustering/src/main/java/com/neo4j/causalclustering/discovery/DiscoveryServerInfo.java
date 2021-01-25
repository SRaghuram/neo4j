/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.Set;

import org.neo4j.kernel.database.DatabaseId;

public interface DiscoveryServerInfo extends CatchupServerAddress, ClientConnector, GroupedServer
{
    Set<DatabaseId> startedDatabaseIds();
}
