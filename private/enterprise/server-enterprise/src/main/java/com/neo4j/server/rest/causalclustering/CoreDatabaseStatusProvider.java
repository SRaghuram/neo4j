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
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.util.Id;

class CoreDatabaseStatusProvider
{
    private final RaftMembershipManager raftMembershipManager;
    private final Health databaseHealth;
    private final TopologyService topologyService;
    private final DurationSinceLastMessageMonitor raftMessageTimerResetMonitor;
    private final RaftMachine raftMachine;
    private final CommandIndexTracker commandIndexTracker;
    private final ThroughputMonitor throughputMonitor;
    private final RoleProvider roleProvider;

    CoreDatabaseStatusProvider( GraphDatabaseAPI db )
    {
        var resolver = db.getDependencyResolver();
        this.raftMembershipManager = resolver.resolveDependency( RaftMembershipManager.class );
        this.databaseHealth = resolver.resolveDependency( DatabaseHealth.class );
        this.topologyService = resolver.resolveDependency( TopologyService.class );
        this.raftMachine = resolver.resolveDependency( RaftMachine.class );
        this.raftMessageTimerResetMonitor = resolver.resolveDependency( DurationSinceLastMessageMonitor.class );
        this.commandIndexTracker = resolver.resolveDependency( CommandIndexTracker.class );
        this.throughputMonitor = resolver.resolveDependency( ThroughputMonitor.class );
        this.roleProvider = resolver.resolveDependency( RoleProvider.class );
    }

    Role currentRole()
    {
        return roleProvider.currentRole();
    }

    ClusteringDatabaseStatusResponse currentStatus()
    {
        var myId = raftMachine.memberId();
        var optLeaderId = getLeader();
        var votingMembers = Set.copyOf( raftMembershipManager.votingMembers() );
        var participatingInRaftGroup = optLeaderId.isPresent() && votingMembers.contains( myId );
        var lastAppliedRaftIndex = commandIndexTracker.getAppliedCommandIndex();
        var millisSinceLastLeaderMessage = Objects.equals( myId, optLeaderId.orElse( null ) ) ? Duration.ZERO :
                                           raftMessageTimerResetMonitor.durationSinceLastMessage();
        var raftCommandsPerSecond = throughputMonitor.throughput().orElse( null );

        return new ClusteringDatabaseStatusResponse( lastAppliedRaftIndex, participatingInRaftGroup, votingMembers, databaseHealth.isHealthy(), myId.uuid(),
                optLeaderId.map( Id::uuid ).orElse( null ), millisSinceLastLeaderMessage, raftCommandsPerSecond, true, topologyService.isHealthy() );
    }

    private Optional<RaftMemberId> getLeader()
    {
        return raftMachine.getLeaderInfo().map( LeaderInfo::memberId );
    }
}
