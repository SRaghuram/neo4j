/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

public class GetDatabaseIdResponse
{
    private final DatabaseId databaseIdRaw;

    public GetDatabaseIdResponse( DatabaseId databaseIdRaw )
    {
        this.databaseIdRaw = databaseIdRaw;
    }

    public DatabaseId databaseId()
    {
        return databaseIdRaw;
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
        GetDatabaseIdResponse that = (GetDatabaseIdResponse) o;
        return Objects.equals( databaseIdRaw, that.databaseIdRaw );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseIdRaw );
    }

    @Override
    public String toString()
    {
        return "GetDatabaseIdResponse{" + "databaseId=" + databaseIdRaw + '}';
    }
}
