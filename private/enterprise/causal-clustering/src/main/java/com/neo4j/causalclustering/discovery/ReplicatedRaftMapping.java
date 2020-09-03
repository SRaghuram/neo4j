/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseServer;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

public class ReplicatedRaftMapping
{
    private final ServerId serverId;
    private final Map<DatabaseId,RaftMemberId> mapping;

    private ReplicatedRaftMapping( ServerId serverId, Map<DatabaseId,RaftMemberId> mapping )
    {
        this.serverId = serverId;
        this.mapping = mapping;
    }

    public static ReplicatedRaftMapping of( ServerId serverId, Map<DatabaseId,RaftMemberId> mapping )
    {
        return new ReplicatedRaftMapping( serverId, mapping );
    }

    public static ReplicatedRaftMapping emptyOf( ServerId serverId )
    {
        return new ReplicatedRaftMapping( serverId, new HashMap<>() );
    }

    public ServerId serverId()
    {
        return serverId;
    }

    public Map<DatabaseId,RaftMemberId> mapping()
    {
        return Collections.unmodifiableMap( mapping );
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
        ReplicatedRaftMapping that = (ReplicatedRaftMapping) o;
        return Objects.equals( serverId, that.serverId ) && Objects.equals( mapping, that.mapping );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( serverId, mapping );
    }

    @Override
    public String toString()
    {
        return String.format( "%s{%s}", serverId, mapping );
    }

    private static ReplicatedRaftMapping add( ReplicatedRaftMapping existing, DatabaseServer key, RaftMemberId raftMemberId )
    {
        if ( existing == null )
        {
            existing = ReplicatedRaftMapping.emptyOf( key.serverId() );
        }
        existing.mapping.put( key.databaseId(), raftMemberId );
        return existing;
    }

    public static Map<ServerId,ReplicatedRaftMapping> of( Map<DatabaseServer,RaftMemberId> map )
    {
        var mappings = new HashMap<ServerId,ReplicatedRaftMapping>();
        map.forEach( ( key, value ) -> mappings.compute( key.serverId(), ( ignored, existing ) -> ReplicatedRaftMapping.add( existing, key, value ) ) );
        return mappings;
    }
}
