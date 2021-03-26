/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.error_handling;

import org.neo4j.kernel.database.Database;


public final class MarkUnhealthyHandler implements DatabasePanicEventHandler
{
    private final Database db;

    MarkUnhealthyHandler( Database db )
    {
        this.db = db;
    }

    public static DatabasePanicEventHandler factory( Database db )
    {
        return new MarkUnhealthyHandler( db );
    }

    @Override
    public void onPanic( DatabasePanicEvent panic )
    {
        var dbHealth = db.getDatabaseHealth();
        if ( dbHealth != null )
        {
            dbHealth.panic( panic.getCause() );
        }
    }
}
