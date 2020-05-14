/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Objects;
import java.util.Set;

import org.neo4j.kernel.database.NamedDatabaseId;

class TransferCandidates
{
    private final NamedDatabaseId namedDatabaseId;
    private final Set<MemberId> members;

    TransferCandidates( NamedDatabaseId namedDatabaseId, Set<MemberId> members )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.members = members;
    }

    public Set<MemberId> members()
    {
        return members;
    }

    public NamedDatabaseId databaseId()
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
        TransferCandidates that = (TransferCandidates) o;
        return Objects.equals( namedDatabaseId, that.namedDatabaseId ) &&
               Objects.equals( members, that.members );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( namedDatabaseId, members );
    }

    @Override
    public String toString()
    {
        return "TransferCandidates{" +
               "namedDatabaseId=" + namedDatabaseId +
               ", members=" + members +
               '}';
    }
}
