/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.MemberInfo;
import org.neo4j.exceptions.KernelException;
import org.neo4j.test.Race;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assume.assumeThat;
import static org.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static org.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static org.neo4j.causalclustering.discovery.RoleInfo.READ_REPLICA;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.assertAllEventualOverviews;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.containsMemberAddresses;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.containsRole;
import static org.neo4j.causalclustering.scenarios.ClusterOverviewHelper.doesNotContainRole;

@RunWith( Parameterized.class )
public class AkkaDiscoveryUncleanShutdownIT
{
    // May be possible to get rid of this with a broadcast + ack instead of a wait for stability
    private static final int SLEEP_BETWEEN_SHUTDOWN_MILLIS = 10 * 1000;

    @Parameterized.Parameter
    public int minimumCoreClusterSizeAtRuntime;

    @Parameterized.Parameters( name = "minimum_core_cluster_size_at_runtime={0}" )
    public static Collection<Integer> data()
    {
        return Arrays.asList( 2, 3 );
    }

    private static final int coreMembers = 3;

    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedCoreParam( CausalClusteringSettings.disable_middleware_logging, "false" )
            .withSharedCoreParam( CausalClusteringSettings.middleware_logging_level, "0" )
            .withDiscoveryServiceType( CommercialDiscoveryServiceType.AKKA_UNCLEAN_SHUTDOWN )
            .withNumberOfCoreMembers( coreMembers )
            .withNumberOfReadReplicas( 0 );

    private Cluster<?> cluster;
    private List<CoreClusterMember> runningCores;
    private Set<Integer> removedCoreIds;

    public void setUp() throws Exception
    {
        cluster = clusterRule
                .withSharedCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_runtime, String.valueOf( minimumCoreClusterSizeAtRuntime ) )
                .startCluster();
        runningCores = IntStream.range( 0, coreMembers )
                .mapToObj( cluster::getCoreMemberById )
                .collect( Collectors.toList() );
        removedCoreIds = new HashSet<>();
    }

    public void tearDown()
    {
        cluster.shutdown();
    }

    private void withCluster( Race.ThrowingRunnable test ) throws Throwable
    {
        setUp();

        test.run();

        tearDown();
    }

    @Test
    public void shouldRestartSecondOfTwoUncleanLeavers() throws Throwable
    {
        withCluster( () ->
        {
            shutdownCore( 0 );
            Thread.sleep( SLEEP_BETWEEN_SHUTDOWN_MILLIS );
            CoreClusterMember toRestart = shutdownCore( 1 );
            startCore( toRestart );
        } );
    }

    @Test
    public void shouldRestartFirstOfTwoUncleanLeavers() throws Throwable
    {
        assumeThat( "Cannot pass with minimum_core_cluster_size_at_runtime=2. Allowing this scenario is the motivation for setting the default to 3",
                minimumCoreClusterSizeAtRuntime, greaterThanOrEqualTo( 3 ) );

        withCluster( () ->
        {
            CoreClusterMember toRestart = shutdownCore( 0 );
            Thread.sleep( SLEEP_BETWEEN_SHUTDOWN_MILLIS );
            shutdownCore( 1 );
            startCore( toRestart );
        } );
    }

    private void startCore( CoreClusterMember newCore ) throws KernelException, InterruptedException
    {
        newCore.start();
        runningCores.add( newCore );
        removedCoreIds.remove( newCore.serverId() );

        assertOverviews();
    }

    private CoreClusterMember shutdownCore( int memberId ) throws KernelException, InterruptedException
    {
        CoreClusterMember core = cluster.getCoreMemberById( memberId );
        core.shutdown();
        runningCores.remove( core );
        removedCoreIds.add( memberId );

        assertOverviews();

        return core;
    }

    private void assertOverviews() throws KernelException, InterruptedException
    {
        int leaderCount = runningCores.size() > 1 ? 1 : 0;
        int followerCount = runningCores.size() - leaderCount;

        Matcher<List<MemberInfo>> expected = allOf(
                containsMemberAddresses( runningCores ),
                containsRole( LEADER, leaderCount ),
                containsRole( FOLLOWER, followerCount ),
                doesNotContainRole( READ_REPLICA ) );

        assertAllEventualOverviews( cluster, expected, removedCoreIds, emptySet() );
    }
}
