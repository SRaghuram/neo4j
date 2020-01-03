/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@ClusterExtension
@TestInstance( PER_METHOD )
class PreElectionIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private final ClusterConfig clusterConfig = clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "2s" )
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
                        .withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, this::firstServerRefusesToBeLeader )
                        .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, TRUE ) ) );
    }

    @Test
    void shouldNotStartAnElectionIfAMinorityOfServersHaveTimedOutOnHeartbeats() throws Exception
    {
        // given
        Cluster cluster = startCluster( clusterConfig );
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 1, TimeUnit.MINUTES );

        // when
        follower.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class ).triggerElection();

        // then
        assertThrows( TimeoutException.class, () -> cluster.awaitCoreMemberWithRole( Role.CANDIDATE, 1, TimeUnit.MINUTES ) );
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
                .withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, this::firstServerRefusesToBeLeader )
                .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, TRUE ) );
        CoreClusterMember oldLeader = cluster.awaitLeader();

        // when
        cluster.removeCoreMember( oldLeader );

        // then
        CoreClusterMember newLeader = cluster.awaitLeader();

        assertThat( newLeader.serverId(), not( equalTo( oldLeader.serverId() ) ) );
    }

    private String firstServerRefusesToBeLeader( int id )
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
