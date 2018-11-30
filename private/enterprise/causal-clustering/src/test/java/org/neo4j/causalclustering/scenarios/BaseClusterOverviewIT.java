/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.MemberInfo;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static org.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static org.neo4j.causalclustering.discovery.RoleInfo.READ_REPLICA;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.assertAllEventualOverviews;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.assertEventualOverview;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.clusterOverview;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.containsAllMemberAddresses;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.containsMemberAddresses;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.containsRole;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith( Parameterized.class )
public abstract class BaseClusterOverviewIT
{

    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
            .withSharedReadReplicaParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
            .withSharedCoreParam( CausalClusteringSettings.disable_middleware_logging, "false" )
            .withSharedReadReplicaParam( CausalClusteringSettings.disable_middleware_logging, "false" )
            .withSharedCoreParam( CausalClusteringSettings.middleware_logging_level, "0" )
            .withSharedReadReplicaParam( CausalClusteringSettings.middleware_logging_level, "0" );

    protected BaseClusterOverviewIT( DiscoveryServiceType discoveryServiceType )
    {
        clusterRule.withDiscoveryServiceType( discoveryServiceType );
    }

    @Test
    public void shouldDiscoverCoreMembers() throws Exception
    {
        // given
        int coreMembers = 3;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( 0 );

        // when
        Cluster<?> cluster = clusterRule.startCluster();

        Matcher<List<MemberInfo>> expected = allOf(
                containsMemberAddresses( cluster.coreMembers() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 ), ClusterOverviewHelper.doesNotContainRole( READ_REPLICA ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverCoreMembersAndReadReplicas() throws Exception
    {
        // given
        int coreMembers = 3;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        int replicaCount = 3;
        clusterRule.withNumberOfReadReplicas( replicaCount );

        // when
        Cluster<?> cluster = clusterRule.startCluster();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, 2 ), containsRole( READ_REPLICA, replicaCount ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverReadReplicasAfterRestartingCores() throws Exception
    {
        // given
        int coreMembers = 3;
        int readReplicas = 3;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( readReplicas );

        // when
        Cluster<?> cluster = clusterRule.startCluster();
        cluster.shutdownCoreMembers();
        cluster.startCoreMembers();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 ), containsRole( READ_REPLICA, readReplicas ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverNewCoreMembers() throws Exception
    {
        // given
        int initialCoreMembers = 3;
        clusterRule.withNumberOfCoreMembers( initialCoreMembers );
        clusterRule.withNumberOfReadReplicas( 0 );

        Cluster<?> cluster = clusterRule.startCluster();

        // when
        int extraCoreMembers = 2;
        int finalCoreMembers = initialCoreMembers + extraCoreMembers;
        IntStream.range( 0, extraCoreMembers ).forEach( idx -> cluster.addCoreMemberWithId( initialCoreMembers + idx ).start() );

        Matcher<List<MemberInfo>> expected = allOf(
                containsMemberAddresses( cluster.coreMembers() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, finalCoreMembers - 1 ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverNewReadReplicas() throws Exception
    {
        // given
        int coreMembers = 3;
        int initialReadReplicas = 2;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( initialReadReplicas );

        Cluster<?> cluster = clusterRule.startCluster();

        // when
        cluster.addReadReplicaWithId( initialReadReplicas ).start();
        cluster.addReadReplicaWithId( initialReadReplicas + 1 ).start();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                containsRole( LEADER, 1 ),
                containsRole( FOLLOWER, coreMembers - 1 ),
                containsRole( READ_REPLICA, initialReadReplicas + 2 ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverRemovalOfReadReplicas() throws Exception
    {
        // given
        int coreMembers = 3;
        int initialReadReplicas = 3;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( initialReadReplicas );

        Cluster<?> cluster = clusterRule.startCluster();

        assertAllEventualOverviews( cluster, containsRole( READ_REPLICA, initialReadReplicas ) );

        // when
        cluster.removeReadReplicaWithMemberId( 0 );
        cluster.removeReadReplicaWithMemberId( 1 );

        // then
        assertAllEventualOverviews( cluster, containsRole( READ_REPLICA, initialReadReplicas - 2 ) );
    }

    @Test
    public void shouldDiscoverRemovalOfCoreMembers() throws Exception
    {
        // given
        int coreMembers = 5;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( 0 );

        Cluster<?> cluster = clusterRule.startCluster();

        assertAllEventualOverviews( cluster, allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 ) ) );

        // when
        cluster.removeCoreMemberWithServerId( 0 );
        cluster.removeCoreMemberWithServerId( 1 );

        // then
        assertAllEventualOverviews( cluster, allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 - 2 ) ),
                asSet( 0, 1 ), Collections.emptySet() );
    }

    @Test
    public void shouldDiscoverRemovalOfReadReplicaThatWasInitiallyAssociatedWithACoreThatWasAlsoRemoved() throws Throwable
    {
        int coreMembers = 3;
        int readReplicas = 6;

        Cluster<?> cluster = clusterRule
                .withNumberOfCoreMembers( coreMembers )
                .withNumberOfReadReplicas( readReplicas )
                .startCluster();

        assertAllEventualOverviews( cluster, allOf(
                containsRole( LEADER, 1 ),
                containsRole( FOLLOWER, coreMembers - 1 ),
                containsRole( READ_REPLICA, readReplicas ) ) );

        cluster.removeCoreMemberWithServerId( 0 );

        assertAllEventualOverviews( cluster, allOf(
                containsRole( LEADER, 1 ),
                containsRole( FOLLOWER, coreMembers - 2 ),
                containsRole( READ_REPLICA, readReplicas ) ) );

        IntStream.range( 0, readReplicas ).parallel().forEach( cluster::removeReadReplicaWithMemberId );

        assertAllEventualOverviews( cluster, allOf(
                containsRole( LEADER, 1 ),
                containsRole( FOLLOWER, coreMembers - 2 ),
                containsRole( READ_REPLICA, 0 ) ) );
    }

    @Test
    public void shouldDiscoverTimeoutBasedLeaderStepdown() throws Exception
    {
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfReadReplicas( 0 );

        Cluster<?> cluster = clusterRule.startCluster();
        List<CoreClusterMember> followers = cluster.getAllMembersWithRole( Role.FOLLOWER );
        CoreClusterMember leader = cluster.getMemberWithRole( Role.LEADER );
        followers.forEach( CoreClusterMember::shutdown );

        assertEventualOverview( containsRole( LEADER, 0 ), leader, "core" );
    }

    @Test
    public void shouldDiscoverGreaterTermBasedLeaderStepdown() throws Exception
    {
        int originalCoreMembers = 3;
        clusterRule.withNumberOfCoreMembers( originalCoreMembers ).withNumberOfReadReplicas( 0 );

        Cluster<?> cluster = clusterRule.startCluster();
        CoreClusterMember leader = cluster.awaitLeader();
        leader.config().augment( CausalClusteringSettings.refuse_to_be_leader, Settings.TRUE );

        List<MemberInfo> preElectionOverview = clusterOverview( leader.database() );

        CoreClusterMember follower = cluster.getMemberWithRole( Role.FOLLOWER );
        follower.raft().triggerElection( Clock.systemUTC() );

        assertEventualOverview( allOf(
                containsRole( LEADER, 1 ),
                containsRole( FOLLOWER, originalCoreMembers - 1 ),
                not( equalTo( preElectionOverview ) ) ), leader, "core" );
    }
}
