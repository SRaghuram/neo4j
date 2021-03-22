/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.core.consensus.DurationSinceLastMessageMonitor;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.util.Id;

import static java.util.stream.Collectors.toMap;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ClusteringDatabaseStatusUtil
{
    private ClusteringDatabaseStatusUtil()
    {
    }

    static StatusMockBuilder coreStatusMockBuilder()
    {
        return new StatusMockBuilder( DbmsInfo.CORE );
    }

    static StatusMockBuilder readReplicaStatusMockBuilder()
    {
        return new StatusMockBuilder( DbmsInfo.READ_REPLICA );
    }

    static StatusMockBuilder standaloneStatusMockBuilder()
    {
        return new StatusMockBuilder( DbmsInfo.ENTERPRISE );
    }

    static class StatusMockBuilder
    {
        private NamedDatabaseId databaseId;
        private Id myself;
        private Id leader;
        private boolean healthy;
        private boolean available;
        private Duration durationSinceLastMessage;
        private long appliedCommandIndex;
        private double throughput;
        private Role role;
        private DbmsInfo dbmsInfo;
        private Map<Id,RoleInfo> coreRoles = new HashMap<>();

        private StatusMockBuilder( DbmsInfo dbmsInfo )
        {
            this.dbmsInfo = dbmsInfo;
        }

        StatusMockBuilder databaseId( NamedDatabaseId databaseId )
        {
            this.databaseId = databaseId;
            return this;
        }

        StatusMockBuilder myself( Id myself )
        {
            this.myself = myself;
            return this;
        }

        StatusMockBuilder leader( Id leader )
        {
            this.leader = leader;
            this.coreRoles.put( leader, RoleInfo.LEADER );
            return this;
        }

        StatusMockBuilder core( Id core )
        {
            this.coreRoles.put( core, RoleInfo.FOLLOWER );
            return this;
        }

        StatusMockBuilder healthy( boolean healthy )
        {
            this.healthy = healthy;
            return this;
        }

        StatusMockBuilder available( boolean available )
        {
            this.available = available;
            return this;
        }

        StatusMockBuilder durationSinceLastMessage( Duration durationSinceLastMessage )
        {
            this.durationSinceLastMessage = durationSinceLastMessage;
            return this;
        }

        StatusMockBuilder appliedCommandIndex( long appliedCommandIndex )
        {
            this.appliedCommandIndex = appliedCommandIndex;
            return this;
        }

        StatusMockBuilder throughput( double throughput )
        {
            this.throughput = throughput;
            return this;
        }

        StatusMockBuilder role( Role role )
        {
            this.role = role;
            return this;
        }

        GraphDatabaseAPI build()
        {
            var db = mock( GraphDatabaseAPI.class );

            when( db.databaseId() ).thenReturn( databaseId );
            when( db.databaseName() ).thenReturn( databaseId.name() );
            when( db.isAvailable( anyLong() ) ).thenReturn( available );
            when( db.dbmsInfo() ).thenReturn( dbmsInfo );

            var resolver = mock( DependencyResolver.class );
            when( db.getDependencyResolver() ).thenReturn( resolver );

            var raftMembershipManager = mock( RaftMembershipManager.class );
            when( raftMembershipManager.votingMembers() )
                    .thenReturn( coreRoles.keySet().stream().map( id -> new RaftMemberId( id.uuid() ) ).collect( Collectors.toSet() ) );
            when( resolver.resolveDependency( RaftMembershipManager.class ) ).thenReturn( raftMembershipManager );

            var databaseHealth = mock( DatabaseHealth.class );
            when( databaseHealth.isHealthy() ).thenReturn( healthy );
            when( resolver.resolveDependency( DatabaseHealth.class ) ).thenReturn( databaseHealth );

            var raftMachine = mock( RaftMachine.class );
            when( raftMachine.memberId() ).thenReturn( new RaftMemberId( myself.uuid() ) );
            when( raftMachine.getLeaderInfo() ).thenReturn( Optional.ofNullable( leader ).map( l -> new LeaderInfo( new RaftMemberId( l.uuid() ), 1 ) ) );
            when( resolver.resolveDependency( RaftMachine.class ) ).thenReturn( raftMachine );

            var durationSinceLastMessageMonitor = mock( DurationSinceLastMessageMonitor.class );
            when( durationSinceLastMessageMonitor.durationSinceLastMessage() ).thenReturn( durationSinceLastMessage );
            when( resolver.resolveDependency( DurationSinceLastMessageMonitor.class ) ).thenReturn( durationSinceLastMessageMonitor );

            var commandIndexTracker = new CommandIndexTracker();
            commandIndexTracker.setAppliedCommandIndex( appliedCommandIndex );
            when( resolver.resolveDependency( CommandIndexTracker.class ) ).thenReturn( commandIndexTracker );

            var throughputMonitor = mock( ThroughputMonitor.class );
            when( throughputMonitor.throughput() ).thenReturn( Optional.of( throughput ) );
            when( resolver.resolveDependency( ThroughputMonitor.class ) ).thenReturn( throughputMonitor );

            var roleProvider = mock( RoleProvider.class );
            when( roleProvider.currentRole() ).thenReturn( role );
            when( resolver.resolveDependency( RoleProvider.class ) ).thenReturn( roleProvider );

            var allCoreServers = coreRoles.entrySet()
                    .stream()
                    .collect( toMap( entry -> new ServerId( entry.getKey().uuid() ), entry -> TestTopology.addressesForCore( entry.hashCode() ) ) );

            var topologyService = mock( TopologyService.class );
            when( resolver.resolveDependency( TopologyService.class ) ).thenReturn( topologyService );
            when( topologyService.allCoreServers() ).thenReturn( allCoreServers );
            when( topologyService.serverId() ).thenReturn( new ServerId( myself.uuid() ) );

            for ( var entry : coreRoles.entrySet() )
            {
                var server = new ServerId( entry.getKey().uuid() );
                var raftMember = new RaftMemberId( entry.getKey().uuid() );
                when( topologyService.lookupRole( databaseId, server ) ).thenReturn( entry.getValue() );
                when( topologyService.resolveServerForRaftMember( raftMember ) ).thenReturn( server );
                if ( entry.getValue() == RoleInfo.LEADER  )
                {
                    when( topologyService.getLeader( databaseId ) ).thenReturn( new LeaderInfo( raftMember, 1 ) );
                }
            }

            return db;
        }
    }
}
