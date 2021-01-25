/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Optional;

import org.neo4j.kernel.database.NamedDatabaseId;

public class LeaderProvider
{
    private final LeaderLocator leaderLocator;
    private final TopologyService topologyService;

    public LeaderProvider( LeaderLocator leaderLocator, TopologyService topologyService )
    {
        this.leaderLocator = leaderLocator;
        this.topologyService = topologyService;
    }

    public RaftMemberId getLeader( NamedDatabaseId databaseId )
    {
        LeaderInfo leadMemberFromDiscovery = topologyService.getLeader( databaseId );
        Optional<LeaderInfo> leadMemberFromLocator = leaderLocator.getLeaderInfo();
        return leadMemberFromLocator
                .filter( leadMember -> leadMemberFromDiscovery == null || leadMember.term() >= leadMemberFromDiscovery.term() )
                .map( LeaderInfo::memberId )
                .orElseGet( () ->
                        leadMemberFromDiscovery == null || leadMemberFromDiscovery.memberId() == null ? null : leadMemberFromDiscovery.memberId()
                );
    }
}
