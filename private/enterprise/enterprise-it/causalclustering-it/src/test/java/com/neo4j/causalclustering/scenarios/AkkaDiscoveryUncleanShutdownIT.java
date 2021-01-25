/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.ClusterOverviewHelper;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.ClusterChecker;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventuallyDoesNotThrow;

@ClusterExtension
@DriverExtension
class AkkaDiscoveryUncleanShutdownIT
{
    // May be possible to get rid of this with a broadcast + ack instead of a wait for stability
    private static final Duration MIN_INTERVAL_BETWEEN_SHUTDOWNS = Duration.ofSeconds( 10 );
    private static final Duration MAX_INTERVAL_BETWEEN_SHUTDOWNS = Duration.ofMinutes( 2 );

    private static final int CORES = 3;

    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private DriverFactory driverFactory;

    private Cluster cluster;
    private List<CoreClusterMember> runningCores;
    private Set<Integer> removedCoreIds;

    @ParameterizedTest( name = "first_core_to_start={0}" )
    @ValueSource( ints = {0, 1, 2} )
    void shouldHandleOneCoreStartingBeforeAllTheOthers( int coreToStartFirst ) throws Exception
    {
        // given
        var clusterConfig = newClusterConfig( 2 );
        cluster = clusterFactory.createCluster( clusterConfig );

        // when
        var started = CompletableFuture.runAsync( cluster.getCoreMemberByIndex( coreToStartFirst )::start );
        Thread.sleep( Duration.ofMinutes( 2 ).toMillis() );
        Cluster.startMembers( cluster.coreMembers().stream().filter( c -> c.index() != coreToStartFirst ).toArray( ClusterMember[]::new ) );
        Cluster.startMembers( cluster.readReplicas().toArray( ClusterMember[]::new ) );

        // then
        assertThat( started ).succeedsWithin( Duration.ofMinutes( 1 ) );
        runningCores = List.copyOf( cluster.coreMembers() );
        removedCoreIds = new HashSet<>();
        assertOverviews();
        checkClusterHealthy();
    }

    @ParameterizedTest( name = "minimum_core_cluster_size_at_runtime={0}" )
    @ValueSource( ints = {2, 3} )
    void shouldRestartSecondOfTwoUncleanLeavers( int minimumCoreClusterSizeAtRuntime ) throws Throwable
    {
        startCluster( minimumCoreClusterSizeAtRuntime );
        checkClusterHealthy();

        shutdownCoreAndWaitForRemoval( 0 );

        // We do not wait for removal here because the core will not be removed from akka because there is no majority
        CoreClusterMember toRestart = shutdownCore( 1 );
        assertOverviews();
        startCore( toRestart );

        assertEventuallyDoesNotThrow( "cluster healthcheck passes", this::checkClusterHealthy, 1, TimeUnit.MINUTES );
    }

    @Test
    void shouldRestartFirstOfTwoUncleanLeavers() throws Throwable
    {
        // Cannot pass with minimum_core_cluster_size_at_runtime=2
        // Allowing this scenario is the motivation for setting the default to 3

        startCluster( 3 );
        checkClusterHealthy();

        CoreClusterMember toRestart = shutdownCoreAndWaitForRemoval( 0 );

        // We do not wait for removal here because the core will not be removed from akka because there is no majority any more
        shutdownCore( 1 );
        assertOverviews();

        startCore( toRestart );

        assertEventuallyDoesNotThrow( "cluster healthcheck passes", this::checkClusterHealthy, 1, TimeUnit.MINUTES );
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

    private Stream<CoreClusterMember> upMembers()
    {
        return cluster.coreMembers().stream().filter( c -> !c.isShutdown() );
    }

    private CoreClusterMember shutdownCoreAndWaitForRemoval( int memberId ) throws InterruptedException
    {
        var deadline = Instant.now().plus( MAX_INTERVAL_BETWEEN_SHUTDOWNS );

        var memberToDown = cluster.getCoreMemberByIndex( memberId );
        var memberToDownAddress = memberToDown.getAkkaCluster().get().selfAddress();
        var member = shutdownCore( memberId );

        assertThat( Instant.now() ).isBefore( deadline ).as( "Core must shut down within the time provided" );

        Supplier<Boolean> shutDownMemberRemoved = () ->
                upMembers().flatMap( c -> c.getAkkaCluster().stream() )
                           .flatMap( c -> StreamSupport.stream( c.state().getMembers().spliterator(), false ) )
                           .noneMatch( m -> m.address().equals( memberToDownAddress ) );

        while ( Instant.now().isBefore( deadline ) )
        {
            if ( shutDownMemberRemoved.get() )
            {
                break;
            }
            Thread.sleep( 500 );
        }

        assertThat( shutDownMemberRemoved.get() ).isTrue();
        assertOverviews();

        return member;
    }

    private CoreClusterMember shutdownCore( int index ) throws InterruptedException
    {
        CoreClusterMember core = cluster.getCoreMemberByIndex( index );
        core.shutdown();
        runningCores.remove( core );
        removedCoreIds.add( index );
        Thread.sleep( MIN_INTERVAL_BETWEEN_SHUTDOWNS.toMillis() );

        return core;
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

    private void assertOverviews()
    {
        int leaderCount = runningCores.size() > 1 ? 1 : 0;
        int followerCount = runningCores.size() - leaderCount;

        Stream<String> databaseNames = Stream.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME );

        var expected = Matchers.allOf(
                ClusterOverviewHelper.containsMemberAddresses( runningCores ),
                Matchers.allOf( databaseNames.map( db -> Matchers.allOf(
                        ClusterOverviewHelper.containsRole( LEADER, db, leaderCount ),
                        ClusterOverviewHelper.containsRole( FOLLOWER, db, followerCount ),
                        ClusterOverviewHelper.doesNotContainRole( READ_REPLICA, db )
                                                   )
                ).collect( Collectors.toList() ) )
        );

        ClusterOverviewHelper.assertAllEventualOverviews( cluster, new HamcrestCondition<>( expected ), removedCoreIds, emptySet() );
    }

    private void checkClusterHealthy() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        try ( ClusterChecker clusterChecker = driverFactory.clusterChecker( cluster ) )
        {
            clusterChecker.verifyConnectivity();
            clusterChecker.verifyClusterStateMatchesOnAllServers();
        }
    }
}
