/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import akka.cluster.MemberStatus;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.Stopwatch;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@TestInstance( PER_METHOD )
@ClusterExtension
class AkkaBindingTimeoutIT
{
    private final ExecutorService executor = Executors.newCachedThreadPool( daemon( "thread-" + this.getClass().getSimpleName() ) );

    @Inject
    private ClusterFactory clusterFactory;

    @AfterEach
    void tearDown() throws Exception
    {
        executor.shutdownNow();
        assertTrue( executor.awaitTermination( 1, MINUTES ) );
    }

    private Cluster createCluster( Map<String,String> extraConfig )
    {
        return clusterFactory.createCluster(
                clusterConfig().withNumberOfCoreMembers( 3 )
                               .withNumberOfReadReplicas( 0 )
                               .withSharedCoreParams( extraConfig )
        );
    }

    private CoreClusterMember getInitialSeedNode( Cluster cluster )
    {
        return cluster.coreMembers().stream().filter( c -> {
            SocketAddress discoveryAdvertisedAddress = c.config().get( CausalClusteringSettings.discovery_advertised_address );
            SocketAddress seedNodeAddress =  c.config().get( CausalClusteringSettings.initial_discovery_members ).get( 0 );
            return seedNodeAddress.equals( discoveryAdvertisedAddress );
        } ).findAny().get();
    }

    @Test
    void notInitialSeedNodesWaitForInitialSeedNodeOnFirstStartup() throws ExecutionException, InterruptedException
    {
        // given
        var longSeedNodeTimeout = Duration.ofMinutes( 1 );
        Map<String,String> extraConfig = Map.of(
                CausalClusteringInternalSettings.middleware_akka_seed_node_timeout.name(), longSeedNodeTimeout.toSeconds() + "s",
                CausalClusteringInternalSettings.cluster_binding_retry_timeout.name(), (longSeedNodeTimeout.toSeconds() * 2) + "s"
        );
        var cluster = createCluster( extraConfig );

        CoreClusterMember initialSeedNode = getInitialSeedNode( cluster );
        List<CoreClusterMember> notInitialSeedNodes = cluster.coreMembers().stream().filter( c -> !initialSeedNode.equals( c ) ).collect( toList() );

        // when
        var stopWatch = Stopwatch.start();
        var startedFirst = startCoreAsync( notInitialSeedNodes.get( 0 ) );

        // then
        // nothing happens
        assertThrows( ConditionTimeoutException.class,
                      () -> verifyAkkaClusterStatus( notInitialSeedNodes.get( 0 ), MemberStatus.up(), longSeedNodeTimeout.toSeconds() + 30, SECONDS ) );
        assertThat( stopWatch.elapsed( SECONDS ) ).isGreaterThan( longSeedNodeTimeout.toSeconds() );

        // when
        stopWatch = Stopwatch.start();
        var startedTheRest = cluster.coreMembers().stream()
                                    .filter( c -> !notInitialSeedNodes.get( 0 ).equals( c ) )
                                    .map( this::startCoreAsync )
                                    .toArray( CompletableFuture<?>[]::new );
        startedFirst.get();
        CompletableFuture.allOf( startedTheRest ).get();

        // then
        verifyNumberOfCoresReportedByTopology( cluster );
        assertThat( stopWatch.elapsed( SECONDS ) ).isLessThan( longSeedNodeTimeout.toSeconds() );
    }

    @Test
    void initialSeedNodeBindsFastOnFirstStartup() throws ExecutionException, InterruptedException
    {
        // given
        var extraLongTimeout = Duration.ofMinutes( 5 );
        Map<String,String> extraConfig = Map.of(
                CausalClusteringInternalSettings.middleware_akka_seed_node_timeout.name(), extraLongTimeout.toSeconds() + "s",
                CausalClusteringInternalSettings.cluster_binding_retry_timeout.name(), (extraLongTimeout.toSeconds() * 2) + "s"
        );
        var cluster = createCluster( extraConfig );

        CoreClusterMember initialSeedNode = getInitialSeedNode( cluster );

        // when
        var stopWatch = Stopwatch.start();
        var startedInitial = startCoreAsync( initialSeedNode );

        // then
        // We have only started one member so it will only get as far as JOINING status because it needs to find a friend in order to get to UP
        verifyAkkaClusterStatus( initialSeedNode, MemberStatus.joining(), 60, SECONDS );
        assertThat( stopWatch.elapsed( SECONDS ) ).isLessThan( extraLongTimeout.toSeconds() );

        // when
        var startedTheRest = cluster.coreMembers().stream()
                                    .filter( c -> !initialSeedNode.equals( c ) )
                                    .map( this::startCoreAsync )
                                    .toArray( CompletableFuture<?>[]::new );
        startedInitial.get();
        CompletableFuture.allOf( startedTheRest ).get();

        // then
        verifyNumberOfCoresReportedByTopology( cluster );
        assertThat( stopWatch.elapsed( SECONDS ) ).isLessThan( extraLongTimeout.toSeconds() );
    }

    @Test
    void initialSeedNodeBindSlowlyOnSecondStartup() throws ExecutionException, InterruptedException
    {
        // given
        var longSeedNodeTimeout = Duration.ofMinutes( 3 );
        Map<String,String> extraConfig = Map.of(
                CausalClusteringInternalSettings.middleware_akka_seed_node_timeout.name(), longSeedNodeTimeout.toSeconds() + "s",
                CausalClusteringInternalSettings.cluster_binding_retry_timeout.name(), (longSeedNodeTimeout.toSeconds() * 2) + "s"
        );
        var cluster = createCluster( extraConfig );

        CoreClusterMember initialSeedNode = getInitialSeedNode( cluster );

        cluster.start();

        // when started then
        verifyNumberOfCoresReportedByTopology( cluster );

        // when
        cluster.shutdown();

        var stopWatch = Stopwatch.start();
        var initialSeedNodeStarted = startCoreAsync( initialSeedNode );
        // We have only started one member so it will only get as far as JOINING status because it needs to find a friend in order to get to UP
        verifyAkkaClusterStatus( initialSeedNode, MemberStatus.joining(),longSeedNodeTimeout.toSeconds() + 60, SECONDS );
        assertThat( stopWatch.elapsed( SECONDS ) ).isGreaterThanOrEqualTo( longSeedNodeTimeout.toSeconds() );

        // when
        stopWatch = Stopwatch.start();
        var startedTheRest = cluster.coreMembers().stream()
                                    .filter( c -> !initialSeedNode.equals( c ) )
                                    .map( this::startCoreAsync )
                                    .toArray( CompletableFuture<?>[]::new );

        initialSeedNodeStarted.get();
        CompletableFuture.allOf( startedTheRest ).get();

        // then
        cluster.coreMembers().forEach( c -> verifyAkkaClusterStatus( c, MemberStatus.up(), 5, SECONDS ) );
        assertThat( stopWatch.elapsed( SECONDS ) ).isLessThan( longSeedNodeTimeout.toSeconds() );
        verifyNumberOfCoresReportedByTopology( cluster );
        assertThat( stopWatch.elapsed( SECONDS ) ).isLessThan( longSeedNodeTimeout.toSeconds() );
    }

    @Test
    void clusterWithInitialSeedNodeForms()
    {
        // given
        var cluster = clusterFactory.createCluster(
                clusterConfig().withNumberOfCoreMembers( 3 )
                               .withNumberOfReadReplicas( 0 )
                               .withSharedCoreParams( Map.of( CausalClusteringSettings.minimum_core_cluster_size_at_runtime.name(), "2" ) )
                               .withSharedCoreParams( Map.of( CausalClusteringSettings.minimum_core_cluster_size_at_formation.name(), "2" ) )
        );

        CoreClusterMember initialSeedNode = getInitialSeedNode( cluster );
        List<CoreClusterMember> notInitialSeedNodes = cluster.coreMembers().stream()
                                                             .filter( c -> !initialSeedNode.equals( c ) )
                                                             .collect( toList() );

        // when
        var startedSeed = startCoreAsync( initialSeedNode );
        var startedOther = startCoreAsync( notInitialSeedNodes.get( 0 ) );
        //then
        assertDoesNotThrow( () -> startedSeed.get( 2, MINUTES ) );
        assertDoesNotThrow( () -> startedOther.get( 1, MINUTES ) );

        //when
        notInitialSeedNodes.get( 1 ).start();

        // then
        verifyNumberOfCoresReportedByTopology( cluster );
    }

    @Test
    void clusterWithoutInitialSeedNodeFailsToForm()
    {
        // given
        var cluster = clusterFactory.createCluster(
                clusterConfig().withNumberOfCoreMembers( 3 )
                               .withNumberOfReadReplicas( 0 )
                               .withSharedCoreParams( Map.of( CausalClusteringSettings.minimum_core_cluster_size_at_runtime.name(), "2" ) )
                               .withSharedCoreParams( Map.of( CausalClusteringSettings.minimum_core_cluster_size_at_formation.name(), "2" ) )
        );

        CoreClusterMember initialSeedNode = getInitialSeedNode( cluster );
        List<CoreClusterMember> notInitialSeedNodes = cluster.coreMembers().stream()
                                                             .filter( c -> !initialSeedNode.equals( c ) )
                                                             .collect( toList() );

        // when
        var started = notInitialSeedNodes.stream()
                                         .map( this::startCoreAsync )
                                         .toArray( CompletableFuture<?>[]::new );
        //then
        assertThrows( TimeoutException.class, () -> CompletableFuture.allOf( started ).get(2, MINUTES) );

        //when
        initialSeedNode.start();

        // then
        verifyNumberOfCoresReportedByTopology( cluster );
    }

    @Test
    void firstTimeClusterStartIsFasterThanSubsequentClusterStarts() throws Exception
    {
        // given
        var longSeedNodeTimeout = Duration.ofMinutes( 1 );
        Map<String,String> extraConfig = Map.of(
                CausalClusteringInternalSettings.middleware_akka_seed_node_timeout.name(), longSeedNodeTimeout.toSeconds() + "s",
                CausalClusteringInternalSettings.cluster_binding_retry_timeout.name(), (longSeedNodeTimeout.toSeconds() * 2) + "s"
        );
        var cluster = clusterFactory.createCluster(
                clusterConfig().withNumberOfCoreMembers( 3 )
                               .withNumberOfReadReplicas( 0 )
                               .withSharedCoreParams( extraConfig )
        );

        var stopWatch = Stopwatch.start();
        cluster.start();

        // when started then
        verifyNumberOfCoresReportedByTopology( cluster );
        long firstStartTime = stopWatch.elapsed(SECONDS);

        // when
        cluster.shutdown();

        stopWatch = Stopwatch.start();
        cluster.start();

        // then
        verifyNumberOfCoresReportedByTopology( cluster );
        long secondStartTime = stopWatch.elapsed(SECONDS);
        assertThat(secondStartTime).isGreaterThan( firstStartTime );
    }

    private CompletableFuture<Void> startCoreAsync( CoreClusterMember core )
    {
        return runAsync( core::start );
    }

    private CompletableFuture<Void> runAsync( Runnable runnable )
    {
        return CompletableFuture.runAsync( runnable, executor );
    }

    private static void verifyAkkaClusterStatus( CoreClusterMember core, MemberStatus status, long timeout, TimeUnit timeUnit )
    {
        assertEventually( () -> core.getAkkaCluster().map( c -> c.selfMember().status() ).orElse( MemberStatus.down() ),
                          equalityCondition( status ),
                          timeout, timeUnit );
    }

    private static void verifyNumberOfCoresReportedByTopology( Cluster cluster )
    {
        List<Integer> expected = List.of( 3, 3, 3 );
        assertEventually( () -> cluster.numberOfCoreMembersReportedByTopologyOnAllCores( DEFAULT_DATABASE_NAME ),
                          equalityCondition( expected ), 30, SECONDS );
    }
}
