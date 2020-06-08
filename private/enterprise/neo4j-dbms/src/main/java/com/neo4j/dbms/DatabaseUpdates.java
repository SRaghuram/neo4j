/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Set;

import org.neo4j.kernel.database.NamedDatabaseId;

class DatabaseUpdates
{
    static final DatabaseUpdates EMPTY = new DatabaseUpdates( Set.of(), Set.of() );

    private final Set<NamedDatabaseId> changed;
    private final Set<NamedDatabaseId> touched;

    DatabaseUpdates( Set<NamedDatabaseId> changed, Set<NamedDatabaseId> touched )
    {
        this.changed = changed;
        this.touched = touched;
    }

    Set<NamedDatabaseId> changed()
    {
        return changed;
    }

    Set<NamedDatabaseId> touched()
    {
        return touched;
    }
}
