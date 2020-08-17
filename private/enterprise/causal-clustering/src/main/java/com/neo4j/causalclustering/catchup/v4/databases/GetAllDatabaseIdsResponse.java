/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import java.util.Objects;
import java.util.Set;

import org.neo4j.kernel.database.NamedDatabaseId;

public class GetAllDatabaseIdsResponse
{
    private final Set<NamedDatabaseId> databaseIds;
    private final int size;

    public GetAllDatabaseIdsResponse( Set<NamedDatabaseId> databaseIds )
    {
        this.databaseIds = databaseIds;
        this.size = databaseIds.size();
    }

    public Set<NamedDatabaseId> databaseIds()
    {
        return databaseIds;
    }

    public int size()
    {
        return size;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        GetAllDatabaseIdsResponse that = (GetAllDatabaseIdsResponse) o;
        return size == that.size &&
               Objects.equals( databaseIds, that.databaseIds );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseIds, size );
    }

}

