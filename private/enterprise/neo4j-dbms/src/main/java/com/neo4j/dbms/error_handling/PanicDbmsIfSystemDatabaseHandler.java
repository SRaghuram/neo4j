/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.error_handling;

public final class PanicDbmsIfSystemDatabaseHandler implements DatabasePanicEventHandler
{
    private final Panicker panicker;

    PanicDbmsIfSystemDatabaseHandler( Panicker panicker )
    {
        this.panicker = panicker;
    }

    public static DatabasePanicEventHandler create( Panicker panicker )
    {
        return new PanicDbmsIfSystemDatabaseHandler( panicker );
    }

    @Override
    public void onPanic( DatabasePanicEvent panic )
    {
        if ( panic.databaseId().isSystemDatabase() )
        {
            panicker.panic( new DbmsPanicEvent( DbmsPanicReason.SystemDbPanicked, panic.getCause() ) );
        }
    }
}
