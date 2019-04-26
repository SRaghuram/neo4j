/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.core.consensus.DurationSinceLastMessageMonitor;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.ws.rs.core.Response;

import org.neo4j.common.DependencyResolver;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.server.rest.repr.OutputFormat;

import static com.neo4j.server.rest.causalclustering.CausalClusteringService.BASE_PATH;

class CoreStatus extends BaseStatus
{
    private final OutputFormat output;

    // Dependency resolved
    private final RaftMembershipManager raftMembershipManager;
    private final Health databaseHealth;
    private final TopologyService topologyService;
    private final DurationSinceLastMessageMonitor raftMessageTimerResetMonitor;
    private final RaftMachine raftMachine;
    private final CommandIndexTracker commandIndexTracker;
    private final ThroughputMonitor throughputMonitor;
    private final RoleProvider roleProvider;

    CoreStatus( OutputFormat output, CoreGraphDatabase db )
    {
        super( output );
        this.output = output;

        DependencyResolver dependencyResolver = db.getDependencyResolver();
        this.raftMembershipManager = dependencyResolver.resolveDependency( RaftMembershipManager.class );
        this.databaseHealth = dependencyResolver.resolveDependency( DatabaseHealth.class );
        this.topologyService = dependencyResolver.resolveDependency( TopologyService.class );
        this.raftMachine = dependencyResolver.resolveDependency( RaftMachine.class );
        this.raftMessageTimerResetMonitor = dependencyResolver.resolveDependency( DurationSinceLastMessageMonitor.class );
        this.commandIndexTracker = dependencyResolver.resolveDependency( CommandIndexTracker.class );
        this.throughputMonitor = dependencyResolver.resolveDependency( ThroughputMonitor.class );
        this.roleProvider = dependencyResolver.resolveDependency( RoleProvider.class );
    }

    @Override
    public Response discover()
    {
        return output.ok( new CausalClusteringDiscovery( BASE_PATH ) );
    }

    @Override
    public Response available()
    {
        return positiveResponse();
    }

    @Override
    public Response readonly()
    {
        Role role = roleProvider.currentRole();
        return ((Role.FOLLOWER == role) || (Role.CANDIDATE == role)) ? positiveResponse() : negativeResponse();
    }

    @Override
    public Response writable()
    {
        return roleProvider.currentRole() == Role.LEADER ? positiveResponse() : negativeResponse();
    }

    @Override
    public Response description()
    {
        MemberId myId = topologyService.memberId();
        MemberId leaderId = getLeader();
        List<MemberId> votingMembers = new ArrayList<>( raftMembershipManager.votingMembers() );
        boolean participatingInRaftGroup = votingMembers.contains( myId ) && Objects.nonNull( leaderId );

        long lastAppliedRaftIndex = commandIndexTracker.getAppliedCommandIndex();
        final Duration millisSinceLastLeaderMessage;
        if ( Objects.equals( myId, leaderId ) )
        {
            millisSinceLastLeaderMessage = Duration.ofMillis( 0 );
        }
        else
        {
            millisSinceLastLeaderMessage = raftMessageTimerResetMonitor.durationSinceLastMessage();
        }

        Double raftCommandsPerSecond = throughputMonitor.throughput().orElse( null );

        return statusResponse( lastAppliedRaftIndex, participatingInRaftGroup, votingMembers, databaseHealth.isHealthy(), myId, leaderId,
                millisSinceLastLeaderMessage, raftCommandsPerSecond, true );
    }

    private MemberId getLeader()
    {
        try
        {
            return raftMachine.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            return null;
        }
    }
}
