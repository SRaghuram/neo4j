/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.database.Database;

class RaiseAvailabilityGuardHandler implements DatabasePanicEventHandler
{
    private static final AvailabilityRequirement PANICKED_REQUIREMENT = () -> PanicException.MESSAGE;

    private final Database db;

    RaiseAvailabilityGuardHandler( Database db )
    {
        this.db = db;
    }

    @Override
    public void onPanic()
    {
        var dbAvailabilityGuard = db.getDatabaseAvailabilityGuard();
        if ( dbAvailabilityGuard != null )
        {
            dbAvailabilityGuard.require( PANICKED_REQUIREMENT );
        }
    }
}
