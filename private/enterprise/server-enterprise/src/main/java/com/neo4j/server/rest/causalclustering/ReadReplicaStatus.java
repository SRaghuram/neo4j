/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;

import java.time.Duration;
import java.util.Collection;
import javax.ws.rs.core.Response;

import org.neo4j.common.DependencyResolver;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.server.rest.repr.OutputFormat;

class ReadReplicaStatus extends ClusterMemberStatus
{
    private final ThroughputMonitor throughputMonitor;

    // Dependency resolved
    private final TopologyService topologyService;
    private final Health dbHealth;
    private final CommandIndexTracker commandIndexTracker;

    ReadReplicaStatus( OutputFormat output, GraphDatabaseAPI databaseAPI, ClusterService clusterService )
    {
        super( output, databaseAPI, clusterService );

        DependencyResolver dependencyResolver = databaseAPI.getDependencyResolver();
        this.commandIndexTracker = dependencyResolver.resolveDependency( CommandIndexTracker.class );
        this.topologyService = dependencyResolver.resolveDependency( TopologyService.class );
        this.dbHealth = dependencyResolver.resolveDependency( DatabaseHealth.class );
        this.throughputMonitor = dependencyResolver.resolveDependency( ThroughputMonitor.class );
    }

    @Override
    public Response available()
    {
        return positiveResponse();
    }

    @Override
    public Response readonly()
    {
        return positiveResponse();
    }

    @Override
    public Response writable()
    {
        return negativeResponse();
    }

    @Override
    public Response description()
    {
        Collection<MemberId> votingMembers = topologyService.allCoreServers().keySet();
        boolean isHealthy = dbHealth.isHealthy();
        boolean isDiscoveryHealthy = topologyService.isHealthy();
        MemberId myId = topologyService.memberId();
        MemberId leaderId = votingMembers.stream()
                .filter( memberId -> topologyService.lookupRole( db.databaseId(), memberId ) == RoleInfo.LEADER )
                .findFirst()
                .orElse( null );
        long lastAppliedRaftIndex = commandIndexTracker.getAppliedCommandIndex();
        // leader message duration is meaningless for replicas since communication is not guaranteed with leader and transactions are streamed periodically
        Duration millisSinceLastLeaderMessage = null;
        Double raftCommandsPerSecond = throughputMonitor.throughput().orElse( null );
        return statusResponse( lastAppliedRaftIndex, false, votingMembers, isHealthy, myId, leaderId, millisSinceLastLeaderMessage,
                raftCommandsPerSecond, false, isDiscoveryHealthy );
    }
}
