/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

class PanicDbmsIfSystemDatabaseHandler implements DatabasePanicEventHandler
{
    private final Panicker panicker;

    PanicDbmsIfSystemDatabaseHandler( Panicker panicker )
    {
        this.panicker = panicker;
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
