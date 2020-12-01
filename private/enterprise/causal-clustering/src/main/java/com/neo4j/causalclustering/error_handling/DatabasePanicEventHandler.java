/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.dbms.ClusterInternalDbmsOperator;

import org.neo4j.kernel.database.Database;

@FunctionalInterface
public interface DatabasePanicEventHandler
{
    /**
     * Since resources may be very limited during a panic any implementation of this interface should be as simple as possible.
     * Don't use up any extra memory or create new threads.
     * @param panic information about the cause and nature of the panic
     */
    void onPanic( DatabasePanicEvent panic );

    static DatabasePanicEventHandler raiseAvailabilityGuard( Database db )
    {
        return new RaiseAvailabilityGuardHandler( db );
    }

    static DatabasePanicEventHandler markUnhealthy( Database db )
    {
        return new MarkUnhealthyHandler( db );
    }

    static DatabasePanicEventHandler stopDatabase( ClusterInternalDbmsOperator internalOperator )
    {
        return new StopDatabaseHandler( internalOperator );
    }

    static DatabasePanicEventHandler panicDbmsIfSystemDatabasePanics( Panicker panicker )
    {
        return new PanicDbmsIfSystemDatabaseHandler( panicker );
    }
}
