/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;

class StubLeaderService implements LeaderService
{
    Map<NamedDatabaseId,RaftMemberId> dbToLeaderMap;

    static StubLeaderService formServers( Map<NamedDatabaseId,MemberId> dbToLeaderMap )
    {
        return new StubLeaderService(
                dbToLeaderMap.keySet().stream().collect( Collectors.toMap( Function.identity(), key -> RaftMemberId.from( dbToLeaderMap.get( key ) ) ) ) );
    }

    StubLeaderService( Map<NamedDatabaseId,RaftMemberId> dbToLeaderMap )
    {
        this.dbToLeaderMap = dbToLeaderMap;
    }

    @Override
    public Optional<MemberId> getLeaderServer( NamedDatabaseId namedDatabaseId )
    {
        return getLeaderId( namedDatabaseId ).map( MemberId::of );
    }

    @Override
    public Optional<SocketAddress> getLeaderBoltAddress( NamedDatabaseId namedDatabaseId )
    {
        return getLeaderId( namedDatabaseId ).map( leader -> new SocketAddress( namedDatabaseId.name(), 7687 ) );
    }

    private Optional<RaftMemberId> getLeaderId( NamedDatabaseId namedDatabaseId )
    {
        return Optional.ofNullable( dbToLeaderMap.get( namedDatabaseId ) );
    }
}
