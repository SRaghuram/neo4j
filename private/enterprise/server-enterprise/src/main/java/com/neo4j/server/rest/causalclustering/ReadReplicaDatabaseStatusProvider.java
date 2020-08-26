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
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;

class ReadReplicaDatabaseStatusProvider
{
    // leader message duration is meaningless for replicas since communication is not guaranteed with leader and transactions are streamed periodically
    private static final Duration MILLIS_SINCE_LAST_LEADER_MESSAGE = null;

    private final NamedDatabaseId databaseId;
    private final TopologyService topologyService;
    private final Health databaseHealth;
    private final CommandIndexTracker commandIndexTracker;
    private final ThroughputMonitor throughputMonitor;

    ReadReplicaDatabaseStatusProvider( GraphDatabaseAPI db )
    {
        var resolver = db.getDependencyResolver();
        this.databaseId = db.databaseId();
        this.commandIndexTracker = resolver.resolveDependency( CommandIndexTracker.class );
        this.topologyService = resolver.resolveDependency( TopologyService.class );
        this.databaseHealth = resolver.resolveDependency( DatabaseHealth.class );
        this.throughputMonitor = resolver.resolveDependency( ThroughputMonitor.class );
    }

    ClusteringDatabaseStatusResponse currentStatus()
    {
        var votingServers = topologyService.allCoreServers().keySet();
        var healthy = databaseHealth.isHealthy();
        var myId = RaftMemberId.from( topologyService.memberId() );
        var leaderId = findLeaderId( votingServers );
        var lastAppliedRaftIndex = commandIndexTracker.getAppliedCommandIndex();
        var raftCommandsPerSecond = throughputMonitor.throughput().orElse( null );
        var votingMembers = votingServers.stream().map( RaftMemberId::from ).collect( Collectors.toSet() );

        return new ClusteringDatabaseStatusResponse( lastAppliedRaftIndex, false, votingMembers, healthy, myId, leaderId,
                                                     MILLIS_SINCE_LAST_LEADER_MESSAGE, raftCommandsPerSecond, false, topologyService.isHealthy() );
    }

    private RaftMemberId findLeaderId( Set<MemberId> votingMembers )
    {
        return votingMembers.stream()
                            .filter( memberId -> topologyService.lookupRole( databaseId, memberId ) == RoleInfo.LEADER )
                            .map( RaftMemberId::from )
                            .findFirst()
                            .orElse( null );
    }
}
