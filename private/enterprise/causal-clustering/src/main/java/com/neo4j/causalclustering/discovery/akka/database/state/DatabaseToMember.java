/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.database.state;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

public class DatabaseToMember
{
    private final DatabaseId databaseId;
    private final MemberId memberId;

    public DatabaseToMember( DatabaseId databaseId, MemberId memberId )
    {
        this.databaseId = databaseId;
        this.memberId = memberId;
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public MemberId memberId()
    {
        return memberId;
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
        DatabaseToMember that = (DatabaseToMember) o;
        return Objects.equals( databaseId, that.databaseId ) && Objects.equals( memberId, that.memberId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, memberId );
    }

    @Override
    public String toString()
    {
        return "DatabaseToMember{" + "databaseId=" + databaseId + ", memberId=" + memberId + '}';
    }
}
