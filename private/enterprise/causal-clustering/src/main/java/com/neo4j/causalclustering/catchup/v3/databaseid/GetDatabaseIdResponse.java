/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

public class GetDatabaseIdResponse
{
    private final DatabaseId databaseId;

    public GetDatabaseIdResponse( DatabaseId databaseId )
    {
        this.databaseId = databaseId;
    }

    public DatabaseId databaseId()
    {
        return databaseId;
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
        return Objects.equals( databaseId, that.databaseId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId );
    }

    @Override
    public String toString()
    {
        return "GetDatabaseIdResponse{" + "databaseId=" + databaseId + '}';
    }
}
