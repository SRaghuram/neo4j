/*
 * Copyright (c) "Neo4j"
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
    void shouldActuallyStartAClusterWithMultiDcLicense()
    {
        assertDoesNotThrow( () -> startCluster(
                clusterConfig
                        .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, TRUE ) ) );
    }

    @Test
    void shouldNotStartAnElectionIfAMinorityOfServersHaveTimedOutOnHeartbeats() throws Exception
    {
        // given
        Cluster cluster = startCluster( clusterConfig );
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( DEFAULT_DATABASE_NAME, Role.FOLLOWER );

        // when
        follower.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class ).triggerElection();

        // then
        Duration maxFailureDetectionTimeout = follower.config().get( CausalClusteringSettings.leader_failure_detection_window ).getMin();
        var testTimeout = maxFailureDetectionTimeout.multipliedBy( 2 ).toSeconds();
        assertThrows(
                TimeoutException.class,
                () -> cluster.awaitCoreMemberWithRole( DEFAULT_DATABASE_NAME, Role.CANDIDATE, testTimeout, TimeUnit.SECONDS ) );
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

        assertThat( newLeader.index(), not( equalTo( oldLeader.index() ) ) );
    }

    private Cluster startCluster( ClusterConfig clusterConfig ) throws Exception
    {
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        return cluster;
    }
}
