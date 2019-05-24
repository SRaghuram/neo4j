/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.monitoring.Health;

/**
 * A ClusteredDatabaseManager allows for the creation and retrieval of new clustered Neo4j databases, as well as some limited lifecycle management.
 */
public interface ClusteredDatabaseManager extends DatabaseManager<ClusteredDatabaseContext>
{
    Health getAllHealthServices();
}
