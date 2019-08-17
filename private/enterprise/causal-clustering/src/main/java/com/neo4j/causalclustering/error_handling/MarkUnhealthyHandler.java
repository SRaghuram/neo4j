/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import org.neo4j.kernel.database.Database;

import static com.neo4j.causalclustering.error_handling.PanicException.EXCEPTION;

class MarkUnhealthyHandler implements DatabasePanicEventHandler
{
    private final Database db;

    MarkUnhealthyHandler( Database db )
    {
        this.db = db;
    }

    @Override
    public void onPanic()
    {
        var dbHealth = db.getDatabaseHealth();
        if ( dbHealth != null )
        {
            dbHealth.panic( EXCEPTION );
        }
    }
}
