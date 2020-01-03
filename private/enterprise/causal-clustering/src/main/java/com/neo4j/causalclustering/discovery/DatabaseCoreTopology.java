/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.Map;
import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class DatabaseCoreTopology implements Topology<CoreServerInfo>
{
    private final DatabaseId databaseId;
    private final RaftId raftId;
    private final Map<MemberId,CoreServerInfo> coreMembers;

    public DatabaseCoreTopology( DatabaseId databaseId, RaftId raftId, Map<MemberId,CoreServerInfo> coreMembers )
    {
        this.databaseId = requireNonNull( databaseId );
        this.raftId = raftId;
        this.coreMembers = Map.copyOf( coreMembers );
    }

    public static DatabaseCoreTopology empty( DatabaseId databaseId )
    {
        return new DatabaseCoreTopology( databaseId, null, emptyMap() );
    }

    @Override
    public Map<MemberId,CoreServerInfo> members()
    {
        return coreMembers;
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public RaftId raftId()
    {
        return raftId;
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
        var that = (DatabaseCoreTopology) o;
        return Objects.equals( databaseId, that.databaseId ) &&
               Objects.equals( raftId, that.raftId ) &&
               Objects.equals( coreMembers, that.coreMembers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, raftId, coreMembers );
    }

    @Override
    public String toString()
    {
        return "DatabaseCoreTopology{" +
               "databaseId=" + databaseId +
               ", raftId=" + raftId +
               ", coreMembers=" + coreMembers +
               '}';
    }
}
