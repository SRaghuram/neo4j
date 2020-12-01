/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import java.util.List;

import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * This service provides methods to get {@link DbmsPanicker} instances which can be used to trigger DBMS Panics and {@link DatabasePanicker} instances which can
 * be used to trigger Database Panics
 */
public interface PanicService
{
    // We don't currently have an interface for adding handlers for DBMS panics but that's simply because we don't need one.

    void addDatabasePanicEventHandlers( NamedDatabaseId namedDatabaseId, List<? extends DatabasePanicEventHandler> handlers );

    void removeDatabasePanicEventHandlers( NamedDatabaseId namedDatabaseId );

    /**
     * Get a panicker.
     *
     * @return the interface for triggering all kinds of panic.
     */
    Panicker panicker();

    /**
     * Get a Database panicker for the requested database.
     *
     * @deprecated use panicker instead with an appropriate Panic instance.
     *
     * @param namedDatabaseId identifier for the database that the returned panicker will be for
     * @return the interface for triggering a Database
     */
    @Deprecated
    DatabasePanicker panickerFor( NamedDatabaseId namedDatabaseId );
}
