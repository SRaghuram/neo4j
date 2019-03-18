/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;

import java.time.Duration;
import java.util.Collection;
import javax.ws.rs.core.Response;

import org.neo4j.common.DependencyResolver;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.server.rest.repr.OutputFormat;

import static com.neo4j.server.rest.causalclustering.CausalClusteringService.BASE_PATH;

class ReadReplicaStatus extends BaseStatus
{
    private final OutputFormat output;

    private final ThroughputMonitor throughputMonitor;

    // Dependency resolved
    private final TopologyService topologyService;
    private final DatabaseHealth dbHealth;
    private final CommandIndexTracker commandIndexTracker;

    ReadReplicaStatus( OutputFormat output, ReadReplicaGraphDatabase db )
    {
        super( output );
        this.output = output;

        DependencyResolver dependencyResolver = db.getDependencyResolver();
        this.commandIndexTracker = dependencyResolver.resolveDependency( CommandIndexTracker.class );
        this.topologyService = dependencyResolver.resolveDependency( TopologyService.class );
        this.dbHealth = dependencyResolver.resolveDependency( DatabaseHealth.class );

        throughputMonitor = dependencyResolver.resolveDependency( ThroughputMonitor.class );
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
        Collection<MemberId> votingMembers = topologyService.allCoreRoles().keySet();
        boolean isHealthy = dbHealth.isHealthy();
        MemberId memberId = topologyService.myself();
        MemberId leader = topologyService.allCoreRoles()
                .keySet()
                .stream()
                .filter( member -> RoleInfo.LEADER.equals( topologyService.allCoreRoles().get( member ) ) )
                .findFirst()
                .orElse( null );
        long lastAppliedRaftIndex = commandIndexTracker.getAppliedCommandIndex();
        // leader message duration is meaningless for replicas since communication is not guaranteed with leader and transactions are streamed periodically
        Duration millisSinceLastLeaderMessage = null;
        Double raftCommandsPerSecond = throughputMonitor.throughput().orElse( null );
        return statusResponse( lastAppliedRaftIndex, false, votingMembers, isHealthy, memberId, leader, millisSinceLastLeaderMessage,
                raftCommandsPerSecond, false );
    }
}
