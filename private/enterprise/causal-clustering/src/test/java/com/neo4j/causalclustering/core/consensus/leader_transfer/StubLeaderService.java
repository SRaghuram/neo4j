/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;

import java.util.Map;
import java.util.Optional;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;

class StubLeaderService implements LeaderService
{
    Map<NamedDatabaseId,MemberId> dbToLeaderMap;

    StubLeaderService( Map<NamedDatabaseId,MemberId> dbToLeaderMap )
    {
        this.dbToLeaderMap = dbToLeaderMap;
    }

    @Override
    public Optional<MemberId> getLeaderId( NamedDatabaseId namedDatabaseId )
    {
        return Optional.ofNullable( dbToLeaderMap.get( namedDatabaseId ) );
    }

    @Override
    public Optional<SocketAddress> getLeaderBoltAddress( NamedDatabaseId namedDatabaseId )
    {
        return getLeaderId( namedDatabaseId ).map( leader -> new SocketAddress( namedDatabaseId.name(), 7687 ) );
    }
}
