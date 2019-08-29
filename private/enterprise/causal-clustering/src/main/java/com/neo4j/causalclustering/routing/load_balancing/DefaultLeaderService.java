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

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class DefaultLeaderService implements LeaderService
{
    private final LeaderLocatorForDatabase leaderLocatorForDatabase;
    private final TopologyService topologyService;
    private final Log log;

    public DefaultLeaderService( LeaderLocatorForDatabase leaderLocatorForDatabase, TopologyService topologyService, LogProvider logProvider )
    {
        this.leaderLocatorForDatabase = leaderLocatorForDatabase;
        this.topologyService = topologyService;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public Optional<MemberId> getLeaderId( DatabaseId databaseId )
    {
        var leaderLocator = leaderLocatorForDatabase.getLeader( databaseId );
        if ( leaderLocator.isPresent() )
        {
            // this cluster member is part of the Raft group for the specified database
            // leader can be located from the Raft state machine
            var leaderId = getLeaderIdFromLeaderLocator( leaderLocator.get() );
            log.debug( "Leader locator %s for database %s returned leader ID %s", leaderLocator, databaseId, leaderId );
            return leaderId;
        }
        else
        {
            // this cluster member does not participate in the Raft group for the specified database
            // lookup the leader ID using the discovery service
            var leaderId = getLeaderIdFromTopologyService( databaseId );
            log.debug( "Topology service for database %s returned leader ID %s", databaseId, leaderId );
            return leaderId;
        }
    }

    @Override
    public Optional<SocketAddress> getLeaderBoltAddress( DatabaseId databaseId )
    {
        var leaderBoltAddress = getLeaderId( databaseId ).flatMap( this::resolveBoltAddress );
        log.debug( "Leader for database %s has Bolt address %s", databaseId, leaderBoltAddress );
        return leaderBoltAddress;
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
        return topologyService.coreTopologyForDatabase( databaseId )
                .members()
                .keySet()
                .stream()
                .filter( memberId -> topologyService.coreRole( databaseId, memberId ) == RoleInfo.LEADER )
                .findFirst();
    }

    private Optional<SocketAddress> resolveBoltAddress( MemberId memberId )
    {
        Map<MemberId,CoreServerInfo> coresById = topologyService.allCoreServers();
        log.debug( "Resolving Bolt address for member %s using %s", memberId, coresById );
        return Optional.ofNullable( coresById.get( memberId ) ).map( ClientConnector::boltAddress );
    }
}
