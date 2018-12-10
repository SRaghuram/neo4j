/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class PreElectionIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "2s" )
            .withSharedCoreParam( CausalClusteringSettings.enable_pre_voting, "true" );

    @Test
    public void shouldActuallyStartAClusterWithPreVoting() throws Exception
    {
        clusterRule.startCluster();
        // pass
    }

    @Test
    public void shouldActuallyStartAClusterWithPreVotingAndARefuseToBeLeader() throws Throwable
    {
        clusterRule
                .withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, this::firstServerRefusesToBeLeader )
                .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, "true" );
        clusterRule.startCluster();
    }

    @Test
    public void shouldNotStartAnElectionIfAMinorityOfServersHaveTimedOutOnHeartbeats() throws Exception
    {
        // given
        Cluster<?> cluster = clusterRule.startCluster();
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 1, TimeUnit.MINUTES );

        // when
        follower.raft().triggerElection( Clock.systemUTC() );

        // then
        try
        {
            cluster.awaitCoreMemberWithRole( Role.CANDIDATE, 1, TimeUnit.MINUTES );
            fail( "Should not have started an election if less than a quorum have timed out" );
        }
        catch ( TimeoutException e )
        {
            // pass
        }
    }

    @Test
    public void shouldStartElectionIfLeaderRemoved() throws Exception
    {
        // given
        Cluster<?> cluster = clusterRule.startCluster();
        CoreClusterMember oldLeader = cluster.awaitLeader();

        // when
        cluster.removeCoreMember( oldLeader );

        // then
        CoreClusterMember newLeader = cluster.awaitLeader();

        assertThat( newLeader.serverId(), not( equalTo( oldLeader.serverId() ) ) );
    }

    @Test
    public void shouldElectANewLeaderIfAServerRefusesToBeLeader() throws Exception
    {
        // given
        clusterRule
                .withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, this::firstServerRefusesToBeLeader )
                .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, "true" );
        Cluster<?> cluster = clusterRule.startCluster();
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
}
