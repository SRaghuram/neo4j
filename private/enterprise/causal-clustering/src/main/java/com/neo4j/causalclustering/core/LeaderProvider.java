/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;

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

    public MemberId getLeader( NamedDatabaseId databaseId )
    {
        LeaderInfo leadMemberFromDiscovery = topologyService.getLeader( databaseId );
        LeaderInfo leadMember = leaderLocator.getLeaderInfo();
        if ( leadMember != null )
        {

            if ( leadMemberFromDiscovery != null &&
                 leadMemberFromDiscovery.term() > leadMember.term() )
            {
                leadMember = leadMemberFromDiscovery;
            }
        }
        else
        {
            if ( leadMemberFromDiscovery == null ||
                 leadMemberFromDiscovery.memberId() == null )
            {
                return null;
            }
            leadMember = leadMemberFromDiscovery;
        }
        return leadMember.memberId();
    }
}
