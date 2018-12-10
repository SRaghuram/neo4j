/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import java.util.Map;
import java.util.Optional;

import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * A DatabaseService allows for the creation and retrieval of new clustered Neo4j databases, as well as some limited lifecycle management.
 */
public interface DatabaseService extends Lifecycle
{
    /**
     * Stop all databases in order to perform a store copy. This will raise a {@link DatabaseAvailabilityGuard} with a more human-readable blocking requirement.
     */
    void stopForStoreCopy() throws Throwable;

    /**
     * Reflects whether the databases aggregated in this service are available, or whether they have been stopped for some reason.
     *
     * @return All databases are currently not stopped
     */
    boolean areAvailable();

    /**
     * Returns a given {@link LocalDatabase} object by name, or `Optional.empty()` if the database does not exist
     *
     * @param databaseName the name of the database to be returned
     * @return optionally, the local database instance with name databaseName
     */
    Optional<? extends LocalDatabase> get( String databaseName );

    /**
     * Create a {@link LocalDatabase} with the given databaseName
     *
     * @param databaseName the name of the database to be created
     */
    LocalDatabase registerDatabase( String databaseName );

    /**
     * Return all {@link LocalDatabase} instances created by this service, associated with their database names.
     *
     * @return a Map from database names to database objects.
     */
    Map<String,? extends LocalDatabase> registeredDatabases();

    /**
     * This method asserts that the databases service is healthy, and if it is not, attempts to throw an exception of type `E`
     *
     * @param cause The class tag for the exception we wish to be thrown if the database service is unhealthy.
     * @param <E> The type of exception we wish to be thrown if the database service is unhealthy
     * @throws E if the database is unhealthy
     */
    <E extends Throwable> void assertHealthy( Class<E> cause ) throws E;
}
