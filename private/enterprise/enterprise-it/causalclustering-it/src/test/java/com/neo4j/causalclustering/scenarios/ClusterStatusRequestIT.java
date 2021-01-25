/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.replication.ClusterStatusResponse;
import com.neo4j.causalclustering.core.replication.ClusterStatusService;
import com.neo4j.causalclustering.core.state.machines.status.Status;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.core.consensus.RaftMessages.StatusResponse;
import static com.neo4j.causalclustering.core.replication.ReplicationResult.Outcome.APPLIED;
import static com.neo4j.causalclustering.core.replication.ReplicationResult.Outcome.NOT_REPLICATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterExtension
public class ClusterStatusRequestIT
{
    @Inject
    private ClusterFactory clusterFactory;

    @Test
    void shouldGetClusterStatusFromTheLeader() throws Exception
    {
        //given a cluster with elected leader and two followers
        var cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig() );
        cluster.start();

        var leader = cluster.awaitLeader();
        var clusterStatus = leader.resolveDependency( GraphDatabaseSettings.DEFAULT_DATABASE_NAME, ClusterStatusService.class );
        var databaseId = leader.databaseId();
        UUID requestID = UUID.randomUUID();

        //when execute cluster status to the leader
        ClusterStatusResponse response = clusterStatus.clusterStatus( requestID ).get();

        //then replication is applied and response contains ok
        assertEquals( 3, response.getResponses().size() );

        Status ok = new Status( Status.Message.OK );
        var expectedResponses =
                cluster.coreMembers().stream().map( m -> new StatusResponse( m.raftMemberIdFor( databaseId ), ok, requestID ) ).collect( Collectors.toList() );

        assertEquals( response.getReplicationResult().outcome(), APPLIED );
        assertThat( response.getResponses() ).containsAll( expectedResponses );
    }

    @Test
    void shouldGetClusterStatusFromTheFollower() throws Exception
    {
        //given a cluster with elected leader and two followers
        var cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig() );
        cluster.start();

        cluster.awaitLeader();
        var follower = cluster.getMemberWithAnyRole( Role.FOLLOWER );
        var clusterStatus = follower.resolveDependency( GraphDatabaseSettings.DEFAULT_DATABASE_NAME, ClusterStatusService.class );
        var databaseId = follower.databaseId();
        UUID requestID = UUID.randomUUID();

        //when execute cluster status to the follower
        ClusterStatusResponse response = clusterStatus.clusterStatus( requestID ).get();

        //then replication is applied and response contains ok
        assertEquals( 3, response.getResponses().size() );

        Status ok = new Status( Status.Message.OK );
        var expectedResponses =
                cluster.coreMembers().stream().map( m -> new StatusResponse( m.raftMemberIdFor( databaseId ), ok, requestID ) ).collect( Collectors.toList() );

        assertEquals( response.getReplicationResult().outcome(), APPLIED );
        assertThat( response.getResponses() ).containsAll( expectedResponses );
    }

    @Test
    void responseShouldIndicateFailedReplication() throws Exception
    {
        //given a cluster with elected leader and two followers
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig().withSharedCoreParam( CausalClusteringSettings.replication_leader_await_timeout, "1s" );
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        var leader = cluster.awaitLeader();

        var clusterStatus = leader.resolveDependency( GraphDatabaseSettings.DEFAULT_DATABASE_NAME, ClusterStatusService.class );
        var guard = leader.resolveDependency( GraphDatabaseSettings.DEFAULT_DATABASE_NAME, DatabaseAvailabilityGuard.class );
        guard.require( () -> "Fake guard" );

        UUID requestID = UUID.randomUUID();

        //when execute cluster status
        ClusterStatusResponse response = clusterStatus.clusterStatus( requestID ).get();

        //then replication is not applied
        assertEquals( response.getReplicationResult().outcome(), NOT_REPLICATED );
        assertThat( response.getResponses() ).isEmpty();
    }
}
