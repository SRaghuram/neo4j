/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterOverviewHelper;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.neo4j.logging.Level;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.causalclustering.discovery.RoleInfo.READ_REPLICA;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class ClusterOverviewIT
{
    private static final String DB = DEFAULT_DATABASE_NAME;

    private final ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
            .withSharedPrimaryParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
            .withSharedReadReplicaParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
            .withSharedPrimaryParam( CausalClusteringSettings.middleware_logging_level, Level.DEBUG.toString() )
            .withSharedReadReplicaParam( CausalClusteringSettings.middleware_logging_level, Level.DEBUG.toString() );

    @Nested
    @ClusterExtension
    class SharedCluster
    {
        @Inject
        private ClusterFactory clusterFactory;

        private Cluster cluster;
        private int initialPrimaryMembers;
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
            initialPrimaryMembers = cluster.primaryMembers().size();
            initialReadReplicas = cluster.readReplicas().size();
        }

        @Test
        void shouldDiscoverCoreMembersAndReadReplicas()
        {
            // when
            Matcher<List<ClusterOverviewHelper.MemberInfo>> expected = Matchers.allOf(
                    ClusterOverviewHelper.containsAllMemberAddresses( cluster.primaryMembers(), cluster.readReplicas() ),
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, initialPrimaryMembers - 1 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, DB, initialReadReplicas ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( expected ) );
        }

        @Test
        void shouldDiscoverReadReplicasAfterRestartingCores() throws Exception
        {
            // when
            cluster.shutdownPrimaryMembers();
            cluster.startCoreMembers();

            Matcher<List<ClusterOverviewHelper.MemberInfo>> expected = Matchers.allOf(
                    ClusterOverviewHelper.containsAllMemberAddresses( cluster.primaryMembers(), cluster.readReplicas() ),
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, initialPrimaryMembers - 1 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, DB, initialReadReplicas ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( expected ) );
        }

        @Test
        void shouldDiscoverNewCoreMembers() throws Exception
        {
            // when
            int extraCoreMembers = 2;
            int finalCoreMembers = initialPrimaryMembers + extraCoreMembers;

            CoreClusterMember[] newCoreMembers =
                    IntStream.range( 0, extraCoreMembers ).mapToObj( ignored -> cluster.newCoreMember() ).toArray( CoreClusterMember[]::new );

            Cluster.startMembers( newCoreMembers );

            Matcher<List<ClusterOverviewHelper.MemberInfo>> expected = Matchers.allOf(
                    ClusterOverviewHelper.containsAllMemberAddresses( cluster.primaryMembers(), cluster.readReplicas() ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, DB, initialReadReplicas ),
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, finalCoreMembers - 1 ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( expected ) );
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
                    ClusterOverviewHelper.containsAllMemberAddresses( cluster.primaryMembers(), cluster.readReplicas() ),
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, initialPrimaryMembers - 1 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, DB, finalReadReplicas ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( expected ) );
        }

        @Test
        void shouldDiscoverRemovalOfReadReplicas()
        {
            ClusterOverviewHelper.assertAllEventualOverviews( cluster,
                    new HamcrestCondition<>( ClusterOverviewHelper.containsRole( READ_REPLICA, DB, initialReadReplicas ) ) );

            // when
            cluster.removeReadReplicas( cluster.readReplicas().stream().limit( 2 ).collect( toList() ) );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster,
                    new HamcrestCondition<>( ClusterOverviewHelper.containsRole( READ_REPLICA, DB, initialReadReplicas - 2 ) ) );
        }

        @Test
        void shouldDiscoverRemovalOfCoreMembers()
        {
            // given
            int coresToRemove = 2;
            assertTrue( initialPrimaryMembers > coresToRemove, "Expected at least " + initialPrimaryMembers + " cores. Found " + initialPrimaryMembers );

            ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, initialPrimaryMembers - 1 ) ) ) );

            // when
            List<CoreClusterMember> primaryToRemove = cluster.primaryMembers().stream().skip( initialPrimaryMembers - coresToRemove ).collect( toList() );
            cluster.removePrimaryMembers( primaryToRemove );

            // then
            ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, initialPrimaryMembers - 1 - coresToRemove ) ) ),
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
            Cluster cluster = clusterFactory.createCluster( clusterConfig.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 6 ) );
            cluster.start();
            int coreMembers = cluster.primaryMembers().size();
            int readReplicas = cluster.readReplicas().size();

            ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, coreMembers - 1 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, DB, readReplicas ) ) ) );

            cluster.removePrimaryMemberWithIndex( getRunningCoreIndex( cluster ) );

            ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, coreMembers - 2 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, DB, readReplicas ) ) ) );

            cluster.removeReadReplicas( cluster.readReplicas() );

            ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, coreMembers - 2 ),
                    ClusterOverviewHelper.containsRole( READ_REPLICA, DB, 0 ) ) ) );
        }

        @Test
        void shouldDiscoverTimeoutBasedLeaderStepdown() throws Exception
        {
            Cluster cluster = clusterFactory.createCluster( clusterConfig.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 ) );

            cluster.start();

            CoreClusterMember leader = cluster.awaitLeader();
            List<CoreClusterMember> followers = cluster.getAllPrimariesWithRole( Role.FOLLOWER );
            cluster.removePrimaryMembers( followers );

            ClusterOverviewHelper.assertEventualOverview( new HamcrestCondition<>( ClusterOverviewHelper.containsRole( LEADER, DB, 0 ) ), leader );
        }

        @Test
        void shouldDiscoverGreaterTermBasedLeaderStepdown() throws Exception
        {
            int coreMembers = 3;
            int readReplicas = 0;
            Cluster cluster = clusterFactory.createCluster( clusterConfig
                    .withNumberOfCoreMembers( coreMembers )
                    .withNumberOfReadReplicas( readReplicas )
                    .withSharedPrimaryParam( CausalClusteringSettings.enable_pre_voting, FALSE ) ); // triggering elections doesn't work otherwise
            cluster.start();

            CoreClusterMember leader = cluster.awaitLeader();

            ClusterOverviewHelper.assertEventualOverview( new HamcrestCondition<>( Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, coreMembers - 1 ) ) ), leader );

            List<ClusterOverviewHelper.MemberInfo> preElectionOverview = ClusterOverviewHelper.clusterOverview( leader.defaultDatabase() );

            CausalClusteringTestHelpers.forceReelection( cluster, DEFAULT_DATABASE_NAME );

            ClusterOverviewHelper.assertEventualOverview( new HamcrestCondition<>( Matchers.allOf(
                    ClusterOverviewHelper.containsRole( LEADER, DB, 1 ),
                    ClusterOverviewHelper.containsRole( FOLLOWER, DB, coreMembers - 1 ),
                    not( equalTo( preElectionOverview ) ) ) ), leader );
        }
    }

    private static int getRunningCoreIndex( Cluster cluster )
    {
        return cluster.primaryMembers().stream().findFirst().orElseThrow( () -> new IllegalStateException( "Unable to find a running core" ) ).index();
    }
}
