/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.kernel.database.NamedDatabaseId;

class StubRaftMembershipResolver implements RaftMembershipResolver
{
    private final Map<RaftMemberId,ServerId> reverseMappings = new HashMap<>();
    private final Map<NamedDatabaseId,Set<CoreServerIdentity>> dbServers = new HashMap<>();

    StubRaftMembershipResolver()
    {}

    StubRaftMembershipResolver( NamedDatabaseId databaseId, CoreServerIdentity... servers )
    {
        add( databaseId, servers );
    }

    StubRaftMembershipResolver add( NamedDatabaseId databaseId, CoreServerIdentity... servers )
    {
        dbServers.put( databaseId, Set.of( servers ) );
        return this;
    }

    @Override
    public Set<ServerId> votingServers( NamedDatabaseId databaseId )
    {
        return dbServers
                .get( databaseId )
                .stream()
                .map( ServerIdentity::serverId )
                .collect( Collectors.toSet() );
    }

    @Override
    public RaftMemberId resolveRaftMemberForServer( NamedDatabaseId databaseId, ServerId to )
    {
        return dbServers
                .get( databaseId )
                .stream()
                .filter( c -> c.serverId().equals( to ) )
                .findFirst()
                .map( c -> c.raftMemberId( databaseId ) )
                .orElse( null );
    }

    @Override
    public ServerId resolveServerForRaftMember( RaftMemberId memberId )
    {
        return reverseMappings.get( memberId );
    }

    public void addReverseMapping( ServerId serverId, RaftMemberId raftMemberId )
    {
        reverseMappings.put( raftMemberId, serverId );
    }
}
