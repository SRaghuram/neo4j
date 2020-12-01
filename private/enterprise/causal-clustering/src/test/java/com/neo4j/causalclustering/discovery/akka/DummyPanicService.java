/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.error_handling.Panicker;

import java.util.List;

import org.neo4j.kernel.database.NamedDatabaseId;

public class DummyPanicService implements PanicService
{
    public static final Panicker PANICKER = panic ->
    {
        throw new UnsupportedOperationException( panic.getCause() );
    };

    @Override
    public void addDatabasePanicEventHandlers( NamedDatabaseId namedDatabaseId, List<? extends DatabasePanicEventHandler> handlers )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeDatabasePanicEventHandlers( NamedDatabaseId namedDatabaseId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Panicker panicker()
    {
        return PANICKER;
    }

    @Override
    public DatabasePanicker panickerFor( NamedDatabaseId namedDatabaseId )
    {
        throw new UnsupportedOperationException();
    }
}
