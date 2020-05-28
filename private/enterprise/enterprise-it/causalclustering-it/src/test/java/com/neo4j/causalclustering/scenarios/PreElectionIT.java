/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
@TestInstance( PER_METHOD )
class PreElectionIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private final ClusterConfig clusterConfig = clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( CausalClusteringSettings.enable_pre_voting, TRUE );

    @Test
    void shouldActuallyStartAClusterWithPreVoting()
    {
        assertDoesNotThrow( () -> startCluster( clusterConfig ) );
    }

    @Test
    void shouldActuallyStartAClusterWithPreVotingAndARefuseToBeLeader()
    {
        assertDoesNotThrow( () -> startCluster(
                clusterConfig
                        .withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, this::trueIfServerIdZero )
                        .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, TRUE ) ) );
    }

    @Test
    void shouldNotStartAnElectionIfAMinorityOfServersHaveTimedOutOnHeartbeats() throws Exception
    {
        // given
        Cluster cluster = startCluster( clusterConfig );
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 1, MINUTES );

        // when
        follower.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class ).triggerElection();

        // then
        Duration maxFailureDetectionTimeout = follower.config().get( CausalClusteringSettings.failure_detection_window ).getMin();
        assertThrows(
                TimeoutException.class,
                () -> cluster.awaitCoreMemberWithRole( Role.CANDIDATE, maxFailureDetectionTimeout.multipliedBy( 2 ).toSeconds(), TimeUnit.SECONDS ) );
    }

    @Test
    void shouldStartElectionIfLeaderRemoved() throws Exception
    {
        // given
        Cluster cluster = startCluster( clusterConfig );
        CoreClusterMember oldLeader = cluster.awaitLeader();

        // when
        cluster.removeCoreMember( oldLeader );

        // then
        CoreClusterMember newLeader = cluster.awaitLeader();

        assertThat( newLeader.serverId(), not( equalTo( oldLeader.serverId() ) ) );
    }

    @Test
    void shouldElectANewLeaderIfAServerRefusesToBeLeader() throws Exception
    {
        // given
        Cluster cluster = startCluster( clusterConfig
                .withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, this::trueIfServerIdZero )
                .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, TRUE ) );

        CoreClusterMember oldLeader = cluster.awaitLeader();
        assertNotEquals( oldLeader.serverId(), 0 );

        int expectedNextLeaderId = oldLeader.serverId() == 1 ? 2 : 1;
        assertEventually( () -> getAppendIndex( cluster.getCoreMemberById( expectedNextLeaderId ) ), i -> i == getAppendIndex( oldLeader ), 1, MINUTES );

        // when
        cluster.removeCoreMember( oldLeader );

        // then
        CoreClusterMember newLeader = cluster.awaitLeader();
        assertEquals( newLeader.serverId(), expectedNextLeaderId );

        assertThat( newLeader.serverId(), not( equalTo( oldLeader.serverId() ) ) );
    }

    private long getAppendIndex( CoreClusterMember oldLeader )
    {
        return oldLeader.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class ).state().appendIndex();
    }

    private String trueIfServerIdZero( int id )
    {
        return id == 0 ? "true" : "false";
    }

    private Cluster startCluster( ClusterConfig clusterConfig ) throws Exception
    {
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        return cluster;
    }
}
