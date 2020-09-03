/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.common;

import java.util.Objects;

import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.Objects.requireNonNull;

public class RaftMemberKnownMessage
{
    private final NamedDatabaseId namedDatabaseId;

    public RaftMemberKnownMessage( NamedDatabaseId namedDatabaseId )
    {
        this.namedDatabaseId = requireNonNull( namedDatabaseId );
    }

    public NamedDatabaseId namedDatabaseId()
    {
        return namedDatabaseId;
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
        var that = (RaftMemberKnownMessage) o;
        return Objects.equals( namedDatabaseId, that.namedDatabaseId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( namedDatabaseId );
    }

    @Override
    public String toString()
    {
        return "RaftMemberKnownMessage{" +
               "databaseId=" + namedDatabaseId +
               '}';
    }
}
