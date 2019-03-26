/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.discovery.ClientConnector;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Optional;

import org.neo4j.helpers.AdvertisedSocketAddress;

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
    public Optional<MemberId> getLeaderId( String databaseName )
    {
        var leaderLocator = leaderLocatorForDatabase( databaseName );
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
            return getLeaderIdFromTopologyService( databaseName );
        }
    }

    @Override
    public Optional<AdvertisedSocketAddress> getLeaderBoltAddress( String databaseName )
    {
        return getLeaderId( databaseName ).flatMap( this::resolveBoltAddress );
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

    private Optional<MemberId> getLeaderIdFromTopologyService( String databaseName )
    {
        var coreRoles = topologyService.allCoreRoles();
        var coreServerInfos = topologyService.allCoreServers()
                // todo: filtering needs to be enabled once discovery contains multi-db and not multi-clustering database names
                // .filterTopologyByDb( databaseName )
                .members();

        for ( var memberId : coreServerInfos.keySet() )
        {
            if ( coreRoles.get( memberId ) == RoleInfo.LEADER )
            {
                return Optional.of( memberId );
            }
        }
        return Optional.empty();
    }

    private Optional<AdvertisedSocketAddress> resolveBoltAddress( MemberId memberId )
    {
        return topologyService.allCoreServers().find( memberId ).map( ClientConnector::boltAddress );
    }

    private LeaderLocator leaderLocatorForDatabase( String databaseName )
    {
        // todo: lookup the correct LeaderLocator for the given database name (maybe use DatabaseManager for this)
        return leaderLocator;
    }
}
