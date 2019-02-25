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
import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import com.neo4j.causalclustering.helpers.ClusterOverviewHelper;
import com.neo4j.causalclustering.readreplica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.causalclustering.discovery.RoleInfo.READ_REPLICA;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;

@ExtendWith( SuppressOutputExtension.class )
public abstract class BaseClusterOverviewIT
{
    private final ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
            .withSharedReadReplicaParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
            .withSharedCoreParam( CausalClusteringSettings.disable_middleware_logging, "false" )
            .withSharedReadReplicaParam( CausalClusteringSettings.disable_middleware_logging, "false" )
            .withSharedCoreParam( CausalClusteringSettings.middleware_logging_level, "0" )
            .withSharedReadReplicaParam( CausalClusteringSettings.middleware_logging_level, "0" );

    protected BaseClusterOverviewIT( DiscoveryServiceType discoveryServiceType )
    {
        clusterConfig.withDiscoveryServiceType( discoveryServiceType );
    }

    @Nested
    @ClusterExtension
    class SharedCluster
    {
        @Inject
        private ClusterFactory clusterFactory;

        private Cluster<?> cluster;
        private int initialCoreMembers;
        private int initialReadReplicas;

        @BeforeAll
        void startCluster() throws ExecutionException, InterruptedException
        {
            cluster = clusterFactory.createCluster( clusterConfig.withNumberOfCoreMembers( 5 ).withNumberOfReadReplicas( 6 ) );
            cluster.start();
        }

        @BeforeEach
        void countInitialMembers()
        {
            initialCoreMembers = cluster.coreMembers().size();
            initialReadReplicas = cluster.readReplicas().size();
        }

        @Test
        void shouldDiscoverCoreMembersAndReadReplicas() throws Exception
        {
            // when
            Matcher<List<ClusterOverviewHelper.MemberInfo>> expected = Matchers.allOf(
                    ClusterOverviewHelper.containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, initialCoreMembers - 1 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, initialReadReplicas ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, expected );
        }

        @Test
        void shouldDiscoverReadReplicasAfterRestartingCores() throws Exception
        {
            // when
            cluster.shutdownCoreMembers();
            cluster.startCoreMembers();

            Matcher<List<ClusterOverviewHelper.MemberInfo>> expected = Matchers.allOf(
                    ClusterOverviewHelper.containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, initialCoreMembers - 1 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, initialReadReplicas ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, expected );
        }

        @Test
        void shouldDiscoverNewCoreMembers() throws Exception
        {
            // when
            int extraCoreMembers = 2;
            int finalCoreMembers = initialCoreMembers + extraCoreMembers;

            CoreClusterMember[] newCoreMembers =
                    IntStream.range( 0, extraCoreMembers ).mapToObj( ignored -> cluster.newCoreMember() ).toArray( CoreClusterMember[]::new );

            Cluster.startMembers( newCoreMembers );

            Matcher<List<ClusterOverviewHelper.MemberInfo>> expected = Matchers.allOf(
                    ClusterOverviewHelper.containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, initialReadReplicas ),
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, finalCoreMembers - 1 ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, expected );
        }

        @Test
        void shouldDiscoverNewReadReplicas() throws Exception
        {
            // when
            int extraReadReplicas = 2;
            int finalReadReplicas = initialReadReplicas + extraReadReplicas;

            ReadReplica[] newReadReplicas =
                    IntStream.range( 0, extraReadReplicas ).mapToObj( ignore -> cluster.newReadReplica() ).toArray( ReadReplica[]::new );

            Cluster.startMembers( newReadReplicas );

            Matcher<List<ClusterOverviewHelper.MemberInfo>> expected = Matchers.allOf(
                    ClusterOverviewHelper.containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, initialCoreMembers - 1 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, finalReadReplicas ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, expected );
        }

        @Test
        void shouldDiscoverRemovalOfReadReplicas() throws Exception
        {
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, ClusterOverviewHelper.containsRole( READ_REPLICA, initialReadReplicas ) );

            // when
            cluster.removeReadReplicas( cluster.readReplicas().stream().limit( 2 ).collect( toList() ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, ClusterOverviewHelper.containsRole( READ_REPLICA, initialReadReplicas - 2 ) );
        }

        @Test
        void shouldDiscoverRemovalOfCoreMembers() throws Exception
        {
            // given
            int coresToRemove = 2;
            assertTrue( initialCoreMembers > coresToRemove, "Expected at least " + initialCoreMembers + " cores. Found " + initialCoreMembers );

            ClusterOverviewHelper.assertAllEventualOverviews( cluster, Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, initialCoreMembers - 1 ) ) );

            // when
            List<CoreClusterMember> coreMembersToRemove = cluster.coreMembers().stream().skip( initialCoreMembers - coresToRemove ).collect( toList() );
            cluster.removeCoreMembers( coreMembersToRemove );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, initialCoreMembers - 1 - coresToRemove ) ),
                    asSet( 0, 1 ), Collections.emptySet() );
        }
    }

    @Nested
    @ClusterExtension
    @TestInstance( TestInstance.Lifecycle.PER_METHOD )
    class UniqueCluster
    {
        @Inject
        private ClusterFactory clusterFactory;

        @Test
        void shouldDiscoverRemovalOfReadReplicaThatWasInitiallyAssociatedWithACoreThatWasAlsoRemoved() throws Throwable
        {
            Cluster<?> cluster = clusterFactory.createCluster( clusterConfig.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 6 ) );
            cluster.start();
            int coreMembers = cluster.coreMembers().size();
            int readReplicas = cluster.readReplicas().size();

            ClusterOverviewHelper.assertAllEventualOverviews( cluster, Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, coreMembers - 1 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, readReplicas ) ) );

            cluster.removeCoreMemberWithServerId( getRunningCoreId( cluster ) );

            ClusterOverviewHelper.assertAllEventualOverviews( cluster, Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, coreMembers - 2 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, readReplicas ) ) );

            cluster.removeReadReplicas( cluster.readReplicas() );

            ClusterOverviewHelper.assertAllEventualOverviews( cluster, Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, coreMembers - 2 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, 0 ) ) );
        }

        @Test
        void shouldDiscoverTimeoutBasedLeaderStepdown() throws Exception
        {
            Cluster<?> cluster = clusterFactory.createCluster( clusterConfig.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 ) );

            cluster.start();

            List<CoreClusterMember> followers = cluster.getAllMembersWithRole( Role.FOLLOWER );
            CoreClusterMember leader = cluster.getMemberWithRole( Role.LEADER );
            cluster.removeCoreMembers( followers );

            ClusterOverviewHelper.assertEventualOverview( ClusterOverviewHelper.containsRole( LEADER, 0 ), leader, "core" );
        }

        @Test
        void shouldDiscoverGreaterTermBasedLeaderStepdown() throws Exception
        {
            Cluster<?> cluster = clusterFactory.createCluster( clusterConfig.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 ) );
            cluster.start();

            int originalCoreMembers = cluster.coreMembers().size();

            CoreClusterMember leader = cluster.awaitLeader();
            leader.config().augment( CausalClusteringSettings.refuse_to_be_leader, Settings.TRUE );

            List<ClusterOverviewHelper.MemberInfo> preElectionOverview = ClusterOverviewHelper.clusterOverview( leader.database() );

            CoreClusterMember follower = cluster.getMemberWithRole( Role.FOLLOWER );
            follower.raft().triggerElection( Clock.systemUTC() );

            ClusterOverviewHelper.assertEventualOverview( Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, originalCoreMembers - 1 ),
                    not( equalTo( preElectionOverview ) ) ), leader, "core" );
        }
    }

    private static int getRunningCoreId( Cluster<?> cluster )
    {
        return cluster.coreMembers().stream().findFirst().orElseThrow( () -> new IllegalStateException( "Unable to find a running core" ) ).serverId();
    }
}
