/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class DatabaseCoreTopology implements Topology<CoreServerInfo>
{
    private final DatabaseId databaseId;
    private final RaftGroupId raftGroupId;
    private final Map<ServerId,CoreServerInfo> coreServers;

    public DatabaseCoreTopology( DatabaseId databaseId, RaftGroupId raftGroupId, Map<ServerId,CoreServerInfo> coreServers )
    {
        this.databaseId = requireNonNull( databaseId );
        this.raftGroupId = raftGroupId;
        this.coreServers = Map.copyOf( coreServers );
    }

    public static DatabaseCoreTopology empty( DatabaseId databaseId )
    {
        return new DatabaseCoreTopology( databaseId, null, emptyMap() );
    }

    @Override
    public Map<ServerId,CoreServerInfo> servers()
    {
        return coreServers;
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public RaftGroupId raftGroupId()
    {
        return raftGroupId;
    }

    public <T> Set<T> resolve( BiFunction<DatabaseId,ServerId,T> resolver )
    {
        return servers().keySet().stream().map( serverId -> resolver.apply( databaseId, serverId ) )
                .filter( Objects::nonNull ).collect( Collectors.toSet() );
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
               Objects.equals( raftGroupId, that.raftGroupId ) &&
               Objects.equals( coreServers, that.coreServers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, raftGroupId, coreServers );
    }

    @Override
    public String toString()
    {
        return format( "DatabaseCoreTopology{%s %s}", databaseId, coreServers.keySet() );
    }
}
