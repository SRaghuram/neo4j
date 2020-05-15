/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.database.NamedDatabaseId;

class DatabaseUpdates
{
    static final DatabaseUpdates EMPTY = new DatabaseUpdates( Set.of(), Set.of() );

    private final Set<NamedDatabaseId> changed;
    private final Set<NamedDatabaseId> dropped;

    DatabaseUpdates( Set<NamedDatabaseId> changed, Set<NamedDatabaseId> dropped )
    {
        this.changed = changed;
        this.dropped = dropped;
    }

    Set<NamedDatabaseId> changed()
    {
        return changed;
    }

    Set<NamedDatabaseId> dropped()
    {
        return dropped;
    }

    Set<NamedDatabaseId> all()
    {
        return Stream.concat( dropped.stream(), changed.stream() )
                     .collect( Collectors.toSet() );
    }
}
