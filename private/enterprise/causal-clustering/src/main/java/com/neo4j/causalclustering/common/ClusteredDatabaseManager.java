/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.monitoring.Health;

/**
 * A ClusteredDatabaseManager allows for the creation and retrieval of new clustered Neo4j databases, as well as some limited lifecycle management.
 */
public interface ClusteredDatabaseManager extends DatabaseManager<ClusteredDatabaseContext>
{
    /**
     * This method asserts that a given database is healthy, and if it is not, attempts to throw an exception of type `E`
     *
     * @param cause The class tag for the exception we wish to be thrown if the database service is unhealthy.
     * @param <E> The type of exception we wish to be thrown if the database is unhealthy
     * @throws E if the database is unhealthy
     */
    <E extends Throwable> void assertHealthy( DatabaseId databaseId, Class<E> cause ) throws E;

    Health getAllHealthServices();
}
