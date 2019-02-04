/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.monitoring.DatabaseHealth;

/**
 * A ClusteredDatabaseManager allows for the creation and retrieval of new clustered Neo4j databases, as well as some limited lifecycle management.
 */
public interface ClusteredDatabaseManager<DB extends ClusteredDatabaseContext> extends DatabaseManager<DB>
{
    /**
     * Stop all databases in order to perform a store copy. This will raise a {@link DatabaseAvailabilityGuard} with a more human-readable blocking requirement.
     */
    void stopForStoreCopy() throws Throwable;

    @Override
    Optional<DB> getDatabaseContext( String databaseName );

    @Override
    DB createDatabase( String databaseName ) throws DatabaseExistsException;

    /**
     * This method asserts that a given database is healthy, and if it is not, attempts to throw an exception of type `E`
     *
     * @param cause The class tag for the exception we wish to be thrown if the database service is unhealthy.
     * @param <E> The type of exception we wish to be thrown if the database is unhealthy
     * @throws E if the database is unhealthy
     */
    <E extends Throwable> void assertHealthy( String databaseName, Class<E> cause ) throws E;

    DatabaseHealth getAllHealthServices();
}
