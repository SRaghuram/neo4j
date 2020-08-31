/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterOverviewHelper;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.neo4j.logging.Level;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.discovery.DiscoveryServiceType.AKKA_UNCLEAN_SHUTDOWN;
import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.causalclustering.discovery.RoleInfo.READ_REPLICA;
import static com.neo4j.configuration.CausalClusteringSettings.middleware_logging_level;
import static com.neo4j.configuration.CausalClusteringSettings.minimum_core_cluster_size_at_runtime;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ClusterExtension
class AkkaDiscoveryUncleanShutdownIT
{
    // May be possible to get rid of this with a broadcast + ack instead of a wait for stability
    private static final Duration INTERVAL_BETWEEN_SHUTDOWN_CALLS = Duration.ofSeconds( 60 );
    private static final Duration MIN_INTERVAL_BETWEEN_SHUTDOWNS = Duration.ofSeconds( 20 );
    private static final Duration MAX_INTERVAL_BETWEEN_SHUTDOWNS = Duration.ofSeconds( 30 );

    private static final int CORES = 3;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;
    private List<CoreClusterMember> runningCores;
    private Set<Integer> removedCoreIds;

    @ParameterizedTest( name = "minimum_core_cluster_size_at_runtime={0}" )
    @ValueSource( ints = {2, 3} )
    void shouldRestartSecondOfTwoUncleanLeavers( int minimumCoreClusterSizeAtRuntime ) throws Throwable
    {
        startCluster( minimumCoreClusterSizeAtRuntime );

        shutdownCoreAndWaitForRemoval( 0 );
        CoreClusterMember toRestart = shutdownCoreAndWaitForRemoval( 1 );
        startCore( toRestart );
    }

    @Test
    void shouldRestartFirstOfTwoUncleanLeavers() throws Throwable
    {
        // Cannot pass with minimum_core_cluster_size_at_runtime=2
        // Allowing this scenario is the motivation for setting the default to 3

        startCluster( 3 );

        CoreClusterMember toRestart = shutdownCoreAndWaitForRemoval( 0 );

        shutdownCoreAndWaitForRemoval( 1 );

        startCore( toRestart );
    }

    private void startCluster( int minimumCoreClusterSizeAtRuntime ) throws Exception
    {
        var clusterConfig = newClusterConfig( minimumCoreClusterSizeAtRuntime );
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        runningCores = IntStream.range( 0, CORES )
                                .mapToObj( cluster::getCoreMemberByIndex )
                                .collect( toList() );
        removedCoreIds = new HashSet<>();

        // sleep to allow cluster to stabilise, elect leaders etc.
        // TODO: may be possible to remove this with better assertions on cluster state
        Thread.sleep( MIN_INTERVAL_BETWEEN_SHUTDOWNS.toMillis() );
    }

    private void startCore( CoreClusterMember newCore )
    {
        newCore.start();
        runningCores.add( newCore );
        removedCoreIds.remove( newCore.index() );

        assertOverviews();
    }

    private CoreClusterMember shutdownCoreAndWaitForRemoval( int memberId ) throws InterruptedException
    {
        var deadline = Instant.now().plus( INTERVAL_BETWEEN_SHUTDOWN_CALLS );
        var member = shutdownCore( memberId );
        assertThat( Instant.now() ).isBefore( deadline ).as( "Core must shut down within the time provided" );

        var remainingTime = Duration.between( Instant.now(), deadline );
        if ( remainingTime.compareTo( MIN_INTERVAL_BETWEEN_SHUTDOWNS ) < 0 )
        {
            Thread.sleep( MIN_INTERVAL_BETWEEN_SHUTDOWNS.toMillis() );
        }
        else if ( remainingTime.compareTo( MAX_INTERVAL_BETWEEN_SHUTDOWNS ) > 0 )
        {
            Thread.sleep( MAX_INTERVAL_BETWEEN_SHUTDOWNS.toMillis() );
        }
        else
        {
            Thread.sleep( remainingTime.toMillis() );
        }

        return member;
    }

    private CoreClusterMember shutdownCore( int index )
    {
        CoreClusterMember core = cluster.getCoreMemberByIndex( index );
        core.shutdown();
        runningCores.remove( core );
        removedCoreIds.add( index );

        assertOverviews();

        return core;
    }

    private void assertOverviews()
    {
        int leaderCount = runningCores.size() > 1 ? 1 : 0;
        int followerCount = runningCores.size() - leaderCount;

        String db = DEFAULT_DATABASE_NAME;

        Matcher<List<ClusterOverviewHelper.MemberInfo>> expected = Matchers.allOf(
                ClusterOverviewHelper.containsMemberAddresses( runningCores ),
                ClusterOverviewHelper.containsRole( LEADER, db, leaderCount ),
                ClusterOverviewHelper.containsRole( FOLLOWER, db, followerCount ),
                ClusterOverviewHelper.doesNotContainRole( READ_REPLICA, db ) );

        ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( expected ), removedCoreIds, emptySet() );
    }

    private static ClusterConfig newClusterConfig( int minimumCoreClusterSizeAtRuntime )
    {
        return clusterConfig()
                .withSharedCoreParam( minimum_core_cluster_size_at_runtime, String.valueOf( minimumCoreClusterSizeAtRuntime ) )
                .withSharedCoreParam( middleware_logging_level, Level.DEBUG.toString() )
                .withDiscoveryServiceType( AKKA_UNCLEAN_SHUTDOWN )
                .withNumberOfCoreMembers( CORES )
                .withNumberOfReadReplicas( 0 );
    }
}
