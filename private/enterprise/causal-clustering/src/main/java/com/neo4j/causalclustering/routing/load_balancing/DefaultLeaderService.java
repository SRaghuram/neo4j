/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.discovery.ClientConnector;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Optional;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;

public class DefaultLeaderService implements LeaderService
{
    private final LeaderLocator leaderLocator;
    private final TopologyService topologyService;

    public DefaultLeaderService( LeaderLocator leaderLocator, TopologyService topologyService )
    {
        this.leaderLocator = leaderLocator;
        this.topologyService = topologyService;
    }

    @Override
    public Optional<MemberId> getLeaderId( DatabaseId databaseId )
    {
        var leaderLocator = leaderLocatorForDatabase( databaseId );
        if ( leaderLocator != null )
        {
            // this cluster member is part of the Raft group for the specified database
            // leader can be located from the Raft state machine
            return getLeaderIdFromLeaderLocator( leaderLocator );
        }
        else
        {
            // this cluster member does not participate in the Raft group for the specified database
            // lookup the leader ID using the discovery service
            return getLeaderIdFromTopologyService( databaseId );
        }
    }

    @Override
    public Optional<AdvertisedSocketAddress> getLeaderBoltAddress( DatabaseId databaseId )
    {
        return getLeaderId( databaseId ).flatMap( this::resolveBoltAddress );
    }

    private static Optional<MemberId> getLeaderIdFromLeaderLocator( LeaderLocator leaderLocator )
    {
        try
        {
            return Optional.of( leaderLocator.getLeader() );
        }
        catch ( NoLeaderFoundException e )
        {
            return Optional.empty();
        }
    }

    private Optional<MemberId> getLeaderIdFromTopologyService( DatabaseId databaseId )
    {
        var coreRoles = topologyService.allCoreRoles();
        var coreServerInfos = topologyService.coreTopologyForDatabase( databaseId )
                .members();

        return coreServerInfos.keySet()
                .stream()
                .filter( memberId -> coreRoles.get( memberId ) == RoleInfo.LEADER )
                .findFirst();
    }

    private Optional<AdvertisedSocketAddress> resolveBoltAddress( MemberId memberId )
    {
        Map<MemberId,CoreServerInfo> coresById = topologyService.allCoreServers();
        return Optional.ofNullable( coresById.get( memberId ) ).map( ClientConnector::boltAddress );
    }

    private LeaderLocator leaderLocatorForDatabase( DatabaseId databaseId )
    {
        // todo: lookup the correct LeaderLocator for the given database name (maybe use DatabaseManager for this)
        return leaderLocator;
    }
}
