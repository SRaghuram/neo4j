/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipThreadLeakageGuard;

import static com.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver.advertisedSocketAddressComparator;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;
import static org.neo4j.internal.helpers.collection.Iterables.last;
import static org.neo4j.test.assertion.Assert.assertEventually;

@SkipThreadLeakageGuard
@ClusterExtension
@Execution( CONCURRENT )
public class ClusterFormationIT
{
    private static final ExecutorService executor = Executors.newCachedThreadPool( daemon( "thread-" + ClusterFormationIT.class.getSimpleName() ) );

    @Inject
    private ClusterFactory clusterFactory;

    @AfterAll
    static void stopExecutor() throws Exception
    {
        executor.shutdownNow();
        assertTrue( executor.awaitTermination( 30, SECONDS ), () -> "Executor " + executor + " did not shutdown in time" );
    }

    @Test
    void shouldBeAbleToAddAndRemoveCoreMembers() throws Exception
    {
        // given
        var cluster = startCluster();

        // when
        var coreMember = getExistingCoreMember( cluster );
        coreMember.shutdown();
        coreMember.start();

        // then
        verifyNumberOfCoresReportedByTopology( 3, cluster );

        // when
        removeCoreMember( cluster );

        // then
        verifyNumberOfCoresReportedByTopology( 2, cluster );

        // when
        cluster.newCoreMember().start();

        // then
        verifyNumberOfCoresReportedByTopology( 3, cluster );
    }

    @Test
    void shouldBeAbleToAddAndRemoveCoreMembersUnderModestLoad() throws Throwable
    {
        // given
        var cluster = startCluster();
        var stop = new AtomicBoolean();
        var loadFuture = executor.submit( () -> applyLoad( cluster, stop ) );

        // when
        var coreMember = getExistingCoreMember( cluster );
        coreMember.shutdown();
        coreMember.start();

        // then
        verifyNumberOfCoresReportedByTopology( 3, cluster );

        // when
        removeCoreMember( cluster );

        // then
        verifyNumberOfCoresReportedByTopology( 2, cluster );

        // when
        cluster.newCoreMember().start();

        // then
        verifyNumberOfCoresReportedByTopology( 3, cluster );

        stop.set( true );
        assertNull( loadFuture.get( 30, SECONDS ) );
    }

    @Test
    void shouldBeAbleToRestartTheCluster() throws Exception
    {
        // given
        var cluster = startCluster();

        // when started then
        verifyNumberOfCoresReportedByTopology( 3, cluster );

        // when
        cluster.shutdown();
        cluster.start();

        // then
        verifyNumberOfCoresReportedByTopology( 3, cluster );

        // when
        removeCoreMember( cluster );

        cluster.newCoreMember().start();
        cluster.shutdown();

        cluster.start();

        // then
        verifyNumberOfCoresReportedByTopology( 3, cluster );
    }

    private Cluster startCluster() throws Exception
    {
        var cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 1 ) );
        cluster.start();
        return cluster;
    }

    private static Void applyLoad( Cluster cluster, AtomicBoolean stop ) throws Exception
    {
        while ( !stop.get() )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                tx.createNode();
                tx.commit();
            } );
            Thread.sleep( 500 );
        }
        return null;
    }

    private static CoreClusterMember getExistingCoreMember( Cluster cluster )
    {
        // return the core listed last in discovery members
        // never return the first core in discovery members because it might be stopped and then Akka cluster can't bootstrap
        var cores = new ArrayList<>( cluster.coreMembers() );
        cores.sort( ClusterFormationIT::compareDiscoveryAddresses );
        return last( cores );
    }

    private static void removeCoreMember( Cluster cluster )
    {
        cluster.removeCoreMember( getExistingCoreMember( cluster ) );
    }

    private static void verifyNumberOfCoresReportedByTopology( int expected, Cluster cluster ) throws InterruptedException
    {
        assertEventually( () -> cluster.numberOfCoreMembersReportedByTopology( DEFAULT_DATABASE_NAME ), is( expected ), 30, SECONDS );
    }

    private static int compareDiscoveryAddresses( CoreClusterMember core1, CoreClusterMember core2 )
    {
        return advertisedSocketAddressComparator().compare( discoveryAddress( core1 ), discoveryAddress( core2 ) );
    }

    private static SocketAddress discoveryAddress( CoreClusterMember core )
    {
        return core.config().get( CausalClusteringSettings.discovery_advertised_address );
    }
}
