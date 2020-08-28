/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.consensus.GlobalLeaderListener;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.discovery.ClientConnector;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class DefaultLeaderService implements LeaderService, GlobalLeaderListener
{
    private final Map<NamedDatabaseId,RaftMemberId> currentLeaders = new ConcurrentHashMap<>();
    private final TopologyService topologyService;
    private final Log log;

    public DefaultLeaderService( TopologyService topologyService, LogProvider logProvider )
    {
        this.topologyService = topologyService;
        this.log = logProvider.getLog( getClass() );
    }

    public LeaderListener createListener( NamedDatabaseId namedDatabaseId )
    {
        return new ListenerForDatabase( namedDatabaseId, this );
    }

    @Override
    public Optional<MemberId> getLeaderServer( NamedDatabaseId namedDatabaseId )
    {
        return currentLeaderAccordingToRaft( namedDatabaseId ).or( () -> leaderFromTopology( namedDatabaseId ) )
                .map( RaftMemberId::serverId );
    }

    private Optional<RaftMemberId> leaderFromTopology( NamedDatabaseId namedDatabaseId )
    {
        // this cluster member does not participate in the Raft group for the specified database
        // lookup the leader ID using the discovery service
        var leaderId = getLeaderIdFromTopologyService( namedDatabaseId );
        log.debug( "Topology service for database %s returned leader ID %s", namedDatabaseId, leaderId );
        return leaderId;
    }

    private Optional<RaftMemberId> currentLeaderAccordingToRaft( NamedDatabaseId namedDatabaseId )
    {
        var leader = Optional.ofNullable( currentLeaders.get( namedDatabaseId ) );
        leader.ifPresent( l ->
        {
            // this cluster member is part of the Raft group for the specified database
            // leader can be located from the Raft state machine
            log.debug( "Leader provided by the raft group for database %s is %s", namedDatabaseId, l );
        } );
        return leader;
    }

    @Override
    public Optional<SocketAddress> getLeaderBoltAddress( NamedDatabaseId namedDatabaseId )
    {
        var leaderBoltAddress = getLeaderServer( namedDatabaseId ).flatMap( this::resolveBoltAddress );
        log.debug( "Leader for database %s has Bolt address %s", namedDatabaseId, leaderBoltAddress );
        return leaderBoltAddress;
    }

    private Optional<RaftMemberId> getLeaderIdFromTopologyService( NamedDatabaseId namedDatabaseId )
    {
        return Optional.ofNullable( topologyService.getLeader( namedDatabaseId ) ).map( LeaderInfo::memberId );
    }

    private Optional<SocketAddress> resolveBoltAddress( MemberId memberId )
    {
        Map<MemberId,CoreServerInfo> coresById = topologyService.allCoreServers();
        log.debug( "Resolving Bolt address for member %s using %s", memberId, coresById );
        return Optional.ofNullable( coresById.get( memberId ) ).map( ClientConnector::boltAddress );
    }

    @Override
    public void onLeaderSwitch( NamedDatabaseId databaseId, LeaderInfo leaderInfo )
    {
        currentLeaders.compute( databaseId, ( namedDatabaseId, memberId ) -> leaderInfo.memberId() );
    }

    @Override
    public void onUnregister( NamedDatabaseId databaseId )
    {
        currentLeaders.remove( databaseId );
    }

    private static class ListenerForDatabase implements LeaderListener
    {
        private final NamedDatabaseId forDatabase;
        private final GlobalLeaderListener globalLeaderListener;

        ListenerForDatabase( NamedDatabaseId forDatabase, GlobalLeaderListener globalLeaderListener )
        {
            this.forDatabase = forDatabase;
            this.globalLeaderListener = globalLeaderListener;
        }

        @Override
        public void onLeaderSwitch( LeaderInfo leaderInfo )
        {
            globalLeaderListener.onLeaderSwitch( forDatabase, leaderInfo );
        }

        @Override
        public void onUnregister()
        {
            globalLeaderListener.onUnregister( forDatabase );
        }
    }
}
