/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import org.neo4j.kernel.database.Database;

class RaiseAvailabilityGuardHandler implements DatabasePanicEventHandler
{
    private static final String PANIC_REQUIREMENT_MESSAGE = "Clustering components have encountered a critical error: ";

    private final Database db;

    RaiseAvailabilityGuardHandler( Database db )
    {
        this.db = db;
    }

    @Override
    public void onPanic( DatabasePanicEvent panic )
    {
        var dbAvailabilityGuard = db.getDatabaseAvailabilityGuard();
        if ( dbAvailabilityGuard != null )
        {
            dbAvailabilityGuard.require( () -> PANIC_REQUIREMENT_MESSAGE + panic.getCause().getMessage() );
        }
    }
}
