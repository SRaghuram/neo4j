/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class DatabaseCoreTopology implements Topology<CoreServerInfo>
{
    private final DatabaseId databaseId;
    private final RaftId raftId;
    private final Map<MemberId,CoreServerInfo> coreServers;

    public DatabaseCoreTopology( DatabaseId databaseId, RaftId raftId, Map<MemberId,CoreServerInfo> coreServers )
    {
        this.databaseId = requireNonNull( databaseId );
        this.raftId = raftId;
        this.coreServers = Map.copyOf( coreServers );
    }

    public static DatabaseCoreTopology empty( DatabaseId databaseId )
    {
        return new DatabaseCoreTopology( databaseId, null, emptyMap() );
    }

    @Override
    public Map<MemberId,CoreServerInfo> servers()
    {
        return coreServers;
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

    public Set<RaftMemberId> members( BiFunction<DatabaseId,MemberId,RaftMemberId> resolver )
    {
        return servers().keySet().stream().map( serverId -> resolver.apply( databaseId, serverId ) ).collect( Collectors.toSet() );
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
               Objects.equals( coreServers, that.coreServers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, raftId, coreServers );
    }

    @Override
    public String toString()
    {
        return format( "DatabaseCoreTopology{%s %s}", databaseId, coreServers.keySet() );
    }
}
