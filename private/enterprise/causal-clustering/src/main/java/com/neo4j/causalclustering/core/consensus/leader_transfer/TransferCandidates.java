/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import java.util.Objects;
import java.util.Set;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

class TransferCandidates
{
    private final NamedDatabaseId namedDatabaseId;
    private final Set<ServerId> servers;

    TransferCandidates( NamedDatabaseId namedDatabaseId, Set<ServerId> servers )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.servers = servers;
    }

    public Set<ServerId> members()
    {
        return servers;
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
               Objects.equals( servers, that.servers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( namedDatabaseId, servers );
    }

    @Override
    public String toString()
    {
        return "TransferCandidates{" +
               "namedDatabaseId=" + namedDatabaseId +
               ", servers=" + servers +
               '}';
    }
}
