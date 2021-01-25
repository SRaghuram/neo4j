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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;

import static java.util.stream.Collectors.toMap;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ClusteringDatabaseStatusUtil
{
    private ClusteringDatabaseStatusUtil()
    {
    }

    static CoreStatusMockBuilder coreStatusMockBuilder()
    {
        return new CoreStatusMockBuilder();
    }

    static ReadReplicaStatusMockBuilder readReplicaStatusMockBuilder()
    {
        return new ReadReplicaStatusMockBuilder();
    }

    static class CoreStatusMockBuilder
    {
        private NamedDatabaseId databaseId;
        private RaftMemberId memberId;
        private RaftMemberId leaderId;
        private boolean healthy;
        private boolean available;
        private Duration durationSinceLastMessage;
        private long appliedCommandIndex;
        private double throughput;
        private Role role;

        private CoreStatusMockBuilder()
        {
        }

        CoreStatusMockBuilder databaseId( NamedDatabaseId databaseId )
        {
            this.databaseId = databaseId;
            return this;
        }

        CoreStatusMockBuilder memberId( RaftMemberId memberId )
        {
            this.memberId = memberId;
            return this;
        }

        CoreStatusMockBuilder leaderId( RaftMemberId leaderId )
        {
            this.leaderId = leaderId;
            return this;
        }

        CoreStatusMockBuilder healthy( boolean healthy )
        {
            this.healthy = healthy;
            return this;
        }

        CoreStatusMockBuilder available( boolean available )
        {
            this.available = available;
            return this;
        }

        CoreStatusMockBuilder durationSinceLastMessage( Duration durationSinceLastMessage )
        {
            this.durationSinceLastMessage = durationSinceLastMessage;
            return this;
        }

        CoreStatusMockBuilder appliedCommandIndex( long appliedCommandIndex )
        {
            this.appliedCommandIndex = appliedCommandIndex;
            return this;
        }

        CoreStatusMockBuilder throughput( double throughput )
        {
            this.throughput = throughput;
            return this;
        }

        CoreStatusMockBuilder role( Role role )
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
            when( db.dbmsInfo() ).thenReturn( DbmsInfo.CORE );

            var resolver = mock( DependencyResolver.class );
            when( db.getDependencyResolver() ).thenReturn( resolver );

            var raftMembershipManager = mock( RaftMembershipManager.class );
            when( raftMembershipManager.votingMembers() ).thenReturn( Set.of( memberId, leaderId ) );
            when( resolver.resolveDependency( RaftMembershipManager.class ) ).thenReturn( raftMembershipManager );

            var databaseHealth = mock( DatabaseHealth.class );
            when( databaseHealth.isHealthy() ).thenReturn( healthy );
            when( resolver.resolveDependency( DatabaseHealth.class ) ).thenReturn( databaseHealth );

            var topologyService = mock( TopologyService.class );
            when( resolver.resolveDependency( TopologyService.class ) ).thenReturn( topologyService );

            var raftMachine = mock( RaftMachine.class );
            when( raftMachine.memberId() ).thenReturn( memberId );
            when( raftMachine.getLeaderInfo() ).thenReturn( Optional.of( new LeaderInfo( leaderId, 1 ) ) );
            when( resolver.resolveDependency( RaftMachine.class ) ).thenReturn( raftMachine );

            var durationSinceLastMessageMonitor = mock( DurationSinceLastMessageMonitor.class );
            when( durationSinceLastMessageMonitor.durationSinceLastMessage() ).thenReturn( durationSinceLastMessage );
            when( resolver.resolveDependency( DurationSinceLastMessageMonitor.class ) ).thenReturn( durationSinceLastMessageMonitor );

            var commandIndexTracker = mock( CommandIndexTracker.class );
            when( commandIndexTracker.getAppliedCommandIndex() ).thenReturn( appliedCommandIndex );
            when( resolver.resolveDependency( CommandIndexTracker.class ) ).thenReturn( commandIndexTracker );

            var throughputMonitor = mock( ThroughputMonitor.class );
            when( throughputMonitor.throughput() ).thenReturn( Optional.of( throughput ) );
            when( resolver.resolveDependency( ThroughputMonitor.class ) ).thenReturn( throughputMonitor );

            var roleProvider = mock( RoleProvider.class );
            when( roleProvider.currentRole() ).thenReturn( role );
            when( resolver.resolveDependency( RoleProvider.class ) ).thenReturn( roleProvider );

            return db;
        }
    }

    static class ReadReplicaStatusMockBuilder
    {
        private NamedDatabaseId databaseId;
        private boolean healthy;
        private boolean available;
        private ServerId readReplicaId;
        private Map<ServerId,RoleInfo> coreRoles = new HashMap<>();
        private long appliedCommandIndex;
        private double throughput;
        private RaftMemberId leaderId;

        private ReadReplicaStatusMockBuilder()
        {
        }

        ReadReplicaStatusMockBuilder databaseId( NamedDatabaseId databaseId )
        {
            this.databaseId = databaseId;
            return this;
        }

        ReadReplicaStatusMockBuilder healthy( boolean healthy )
        {
            this.healthy = healthy;
            return this;
        }

        ReadReplicaStatusMockBuilder available( boolean available )
        {
            this.available = available;
            return this;
        }

        ReadReplicaStatusMockBuilder readReplicaId( ServerId readReplicaId )
        {
            this.readReplicaId = readReplicaId;
            return this;
        }

        ReadReplicaStatusMockBuilder coreRole( ServerId coreId, RoleInfo role )
        {
            this.coreRoles.put( coreId, role );
            return this;
        }

        ReadReplicaStatusMockBuilder leader( RaftMemberId leaderId )
        {
            this.leaderId = leaderId;
            return this;
        }

        ReadReplicaStatusMockBuilder appliedCommandIndex( long appliedCommandIndex )
        {
            this.appliedCommandIndex = appliedCommandIndex;
            return this;
        }

        ReadReplicaStatusMockBuilder throughput( double throughput )
        {
            this.throughput = throughput;
            return this;
        }

        GraphDatabaseAPI build()
        {
            var db = mock( GraphDatabaseAPI.class );

            when( db.databaseId() ).thenReturn( databaseId );
            when( db.databaseName() ).thenReturn( databaseId.name() );
            when( db.isAvailable( anyLong() ) ).thenReturn( available );
            when( db.dbmsInfo() ).thenReturn( DbmsInfo.READ_REPLICA );

            var resolver = mock( DependencyResolver.class );
            when( db.getDependencyResolver() ).thenReturn( resolver );

            var databaseHealth = mock( DatabaseHealth.class );
            when( databaseHealth.isHealthy() ).thenReturn( healthy );
            when( resolver.resolveDependency( DatabaseHealth.class ) ).thenReturn( databaseHealth );

            var topologyService = mock( TopologyService.class );
            when( topologyService.serverId() ).thenReturn( readReplicaId );

            var allCoreServers = coreRoles.entrySet()
                                          .stream()
                                          .collect( toMap( Entry::getKey, entry -> TestTopology.addressesForCore( entry.hashCode() ) ) );

            when( topologyService.allCoreServers() ).thenReturn( allCoreServers );

            for ( var entry : coreRoles.entrySet() )
            {
                var raftMember = new RaftMemberId( entry.getKey().uuid() );
                when( topologyService.lookupRole( databaseId, entry.getKey() ) ).thenReturn( entry.getValue() );
                when( topologyService.resolveServerForRaftMember( raftMember ) ).thenReturn( entry.getKey() );
                if ( entry.getValue() == RoleInfo.LEADER && leaderId == null )
                {
                    when( topologyService.getLeader( databaseId ) ).thenReturn( new LeaderInfo( raftMember, 1 ) );
                }
            }
            if ( leaderId != null )
            {
                when( topologyService.getLeader( databaseId ) ).thenReturn( new LeaderInfo( leaderId, 1 ) );
            }

            when( resolver.resolveDependency( TopologyService.class ) ).thenReturn( topologyService );

            var commandIndexTracker = mock( CommandIndexTracker.class );
            when( commandIndexTracker.getAppliedCommandIndex() ).thenReturn( appliedCommandIndex );
            when( resolver.resolveDependency( CommandIndexTracker.class ) ).thenReturn( commandIndexTracker );

            var throughputMonitor = mock( ThroughputMonitor.class );
            when( throughputMonitor.throughput() ).thenReturn( Optional.of( throughput ) );
            when( resolver.resolveDependency( ThroughputMonitor.class ) ).thenReturn( throughputMonitor );

            return db;
        }
    }
}
