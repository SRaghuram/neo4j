/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.readreplica.ReadReplica;
import org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.MemberInfo;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.causalclustering.ClusterConfig;
import org.neo4j.test.causalclustering.ClusterExtension;
import org.neo4j.test.causalclustering.ClusterFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static org.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static org.neo4j.causalclustering.discovery.RoleInfo.READ_REPLICA;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.assertAllEventualOverviews;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.assertEventualOverview;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.clusterOverview;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.containsAllMemberAddresses;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.containsRole;
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

        @BeforeAll
        void startCluster() throws ExecutionException, InterruptedException
        {
            cluster = clusterFactory.createCluster( clusterConfig.withNumberOfCoreMembers( 5 ).withNumberOfReadReplicas( 6 ) );
            cluster.start();
        }

        @Test
        void shouldDiscoverCoreMembersAndReadReplicas() throws Exception
        {
            // given
            int replicaCount = cluster.readReplicas().size();

            // when
            Matcher<List<MemberInfo>> expected =
                    allOf( containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ), containsRole( LEADER, 1 ), containsRole( FOLLOWER, 2 ),
                            containsRole( READ_REPLICA, replicaCount ) );

            // then
            assertAllEventualOverviews( cluster, expected );
        }

        @Test
        void shouldDiscoverReadReplicasAfterRestartingCores() throws Exception
        {
            // given
            int coreMembers = cluster.coreMembers().size();
            int readReplicas = cluster.readReplicas().size();

            // when
            cluster.shutdownCoreMembers();
            cluster.startCoreMembers();

            Matcher<List<MemberInfo>> expected = allOf( containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ), containsRole( LEADER, 1 ),
                    containsRole( FOLLOWER, coreMembers - 1 ), containsRole( READ_REPLICA, readReplicas ) );

            // then
            assertAllEventualOverviews( cluster, expected );
        }

        @Test
        void shouldDiscoverNewCoreMembers() throws Exception
        {
            // given
            int initialCoreMembers = cluster.coreMembers().size();
            int readReplicas = cluster.readReplicas().size();

            // when
            int extraCoreMembers = 2;
            int finalCoreMembers = initialCoreMembers + extraCoreMembers;
            IntStream
                    .range( 0, extraCoreMembers )
                    .mapToObj( ignored -> cluster.newCoreMember() )
                    .parallel()
                    .forEach( CoreClusterMember::start );

            Matcher<List<MemberInfo>> expected =
                    allOf( containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ), containsRole( READ_REPLICA, readReplicas ),
                            containsRole( LEADER, 1 ), containsRole( FOLLOWER, finalCoreMembers - 1 ) );

            // then
            assertAllEventualOverviews( cluster, expected );
        }

        @Test
        void shouldDiscoverNewReadReplicas() throws Exception
        {
            // given
            int coreMembers = cluster.coreMembers().size();
            int initialReadReplicas = cluster.readReplicas().size();

            // when
            IntStream.range( 0, 2 ).mapToObj( ignore -> cluster.newReadReplica() ).parallel().forEach( ReadReplica::start );

            Matcher<List<MemberInfo>> expected = allOf( containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ), containsRole( LEADER, 1 ),
                    containsRole( FOLLOWER, coreMembers - 1 ), containsRole( READ_REPLICA, initialReadReplicas + 2 ) );

            // then
            assertAllEventualOverviews( cluster, expected );
        }

        @Test
        void shouldDiscoverRemovalOfReadReplicas() throws Exception
        {
            // given
            int initialReadReplicas = cluster.readReplicas().size();

            assertAllEventualOverviews( cluster, containsRole( READ_REPLICA, initialReadReplicas ) );

            // when
            cluster.removeReadReplicaWithMemberId( getRunningReadReplicaId( cluster ) );
            cluster.removeReadReplicaWithMemberId( getRunningReadReplicaId( cluster ) );

            // then
            assertAllEventualOverviews( cluster, containsRole( READ_REPLICA, initialReadReplicas - 2 ) );
        }

        @Test
        void shouldDiscoverRemovalOfCoreMembers() throws Exception
        {
            // given
            int coresToRemove = 2;
            int coreMembers = cluster.coreMembers().size();
            assertTrue(coreMembers > coresToRemove, "Expected at least " + coreMembers + " cores. Found " + coreMembers);

            assertAllEventualOverviews( cluster, allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 ) ) );

            // when
            cluster.coreMembers().stream().skip( coreMembers - coresToRemove ).parallel().forEach( core -> cluster.removeCoreMember( core ) );

            // then
            assertAllEventualOverviews( cluster, allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 - coresToRemove ) ), asSet( 0, 1 ),
                    Collections.emptySet() );
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

            assertAllEventualOverviews( cluster,
                    allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 ), containsRole( READ_REPLICA, readReplicas ) ) );

            cluster.removeCoreMemberWithServerId( getRunningCoreId( cluster ) );

            assertAllEventualOverviews( cluster,
                    allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 2 ), containsRole( READ_REPLICA, readReplicas ) ) );

            cluster.readReplicas().parallelStream().forEach( cluster::removeReadReplica );

            assertAllEventualOverviews( cluster,
                    allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 2 ), containsRole( READ_REPLICA, 0 ) ) );
        }

        @Test
        void shouldDiscoverTimeoutBasedLeaderStepdown() throws Exception
        {
            Cluster<?> cluster = clusterFactory.createCluster( clusterConfig.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 ) );

            cluster.start();

            List<CoreClusterMember> followers = cluster.getAllMembersWithRole( Role.FOLLOWER );
            CoreClusterMember leader = cluster.getMemberWithRole( Role.LEADER );
            cluster.removeCoreMembers( followers );

            assertEventualOverview( containsRole( LEADER, 0 ), leader, "core" );
        }

        @Test
        void shouldDiscoverGreaterTermBasedLeaderStepdown() throws Exception
        {
            Cluster<?> cluster = clusterFactory.createCluster( clusterConfig.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 ) );
            cluster.start();

            int originalCoreMembers = cluster.coreMembers().size();

            CoreClusterMember leader = cluster.awaitLeader();
            leader.config().augment( CausalClusteringSettings.refuse_to_be_leader, Settings.TRUE );

            List<MemberInfo> preElectionOverview = clusterOverview( leader.database() );

            CoreClusterMember follower = cluster.getMemberWithRole( Role.FOLLOWER );
            follower.raft().triggerElection( Clock.systemUTC() );

            assertEventualOverview(
                    allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, originalCoreMembers - 1 ), not( equalTo( preElectionOverview ) ) ), leader,
                    "core" );
        }
    }

    private static int getRunningCoreId( Cluster<?> cluster )
    {
        return cluster.coreMembers().stream().findFirst().orElseThrow( () -> new IllegalStateException( "Unale to find a running core" ) ).serverId();
    }

    private static int getRunningReadReplicaId( Cluster<?> cluster )
    {
        return cluster.readReplicas().stream().findFirst().orElseThrow( () -> new IllegalStateException( "Could not find a running read replica" ) ).serverId();
    }
}
