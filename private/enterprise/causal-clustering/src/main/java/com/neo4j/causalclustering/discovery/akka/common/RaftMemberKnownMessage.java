/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.common;

import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Objects;

import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.Objects.requireNonNull;

public class RaftMemberKnownMessage
{
    private final NamedDatabaseId namedDatabaseId;
    private final RaftMemberId raftMemberId;

    public RaftMemberKnownMessage( NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId )
    {
        this.namedDatabaseId = requireNonNull( namedDatabaseId );
        this.raftMemberId = raftMemberId;
    }

    public NamedDatabaseId namedDatabaseId()
    {
        return namedDatabaseId;
    }

    @Override
    public String toString()
    {
        return "RaftMemberKnownMessage{" +
               "namedDatabaseId=" + namedDatabaseId +
               ", raftMemberId=" + raftMemberId +
               '}';
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

        RaftMemberKnownMessage that = (RaftMemberKnownMessage) o;

        if ( !Objects.equals( namedDatabaseId, that.namedDatabaseId ) )
        {
            return false;
        }
        return raftMemberId != null ? raftMemberId.equals( that.raftMemberId ) : that.raftMemberId == null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( namedDatabaseId, raftMemberId );
    }

    public RaftMemberId raftMemberId()
    {
        return raftMemberId;
    }
}
