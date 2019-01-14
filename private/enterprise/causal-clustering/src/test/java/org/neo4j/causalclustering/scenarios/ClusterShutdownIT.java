/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.causalclustering.ClusterExtension;
import org.neo4j.test.causalclustering.ClusterFactory;
import org.neo4j.test.extension.Inject;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.test.causalclustering.ClusterConfig.clusterConfig;

@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class ClusterShutdownIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster<?> cluster;

    static Stream<Collection<Integer>> shutdownOrders()
    {
        return Stream.of( asList( 0, 1, 2 ), asList( 1, 2, 0 ), asList( 2, 0, 1 ) );
    }

    @BeforeEach
    void startCluster() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 ) );
        cluster.start();
    }

    @ParameterizedTest
    @MethodSource( "shutdownOrders" )
    void shouldShutdownEvenThoughWaitingForLock( Collection<Integer> shutdownOrder ) throws Exception
    {
        CoreClusterMember leader = cluster.awaitLeader();
        shouldShutdownEvenThoughWaitingForLock0( cluster, leader.serverId(), shutdownOrder );
    }

    private void createANode( AtomicReference<Node> node ) throws Exception
    {
        cluster.coreTx( ( coreGraphDatabase, transaction ) ->
        {
            node.set( coreGraphDatabase.createNode() );
            transaction.success();
        } );
    }

    private void shouldShutdownEvenThoughWaitingForLock0( Cluster<?> cluster, int victimId, Collection<Integer> shutdownOrder ) throws Exception
    {
        final int LONG_TIME = 60_000;
        final int NUMBER_OF_LOCK_ACQUIRERS = 2;

        final ExecutorService txExecutor = Executors.newCachedThreadPool(); // Blocking transactions are executed in
        // parallel, not on the main thread.
        final ExecutorService shutdownExecutor = Executors.newFixedThreadPool( 1 ); // Shutdowns are executed
        // serially, not on the main thread.

        final CountDownLatch acquiredLocksCountdown = new CountDownLatch( NUMBER_OF_LOCK_ACQUIRERS );
        final CountDownLatch locksHolder = new CountDownLatch( 1 );
        final AtomicReference<Node> node = new AtomicReference<>();

        CompletableFuture<Void> preShutdown = new CompletableFuture<>();

        // set shutdown order
        CompletableFuture<Void> afterShutdown = preShutdown;
        for ( Integer id : shutdownOrder )
        {
            afterShutdown = afterShutdown.thenRunAsync( () -> cluster.getCoreMemberById( id ).shutdown(), shutdownExecutor );
        }

        createANode( node );

        try
        {
            // when - blocking on lock acquiring
            final GraphDatabaseService leader = cluster.getCoreMemberById( victimId ).database();

            for ( int i = 0; i < NUMBER_OF_LOCK_ACQUIRERS; i++ )
            {
                txExecutor.execute( () ->
                {
                    try ( Transaction tx = leader.beginTx() )
                    {
                        acquiredLocksCountdown.countDown();
                        tx.acquireWriteLock( node.get() );
                        locksHolder.await();
                        tx.success();
                    }
                    catch ( Exception e )
                    {
                        /* Since we are shutting down, a plethora of possible exceptions are expected. */
                    }
                } );
            }

            // await locks
            if ( !acquiredLocksCountdown.await( LONG_TIME, MILLISECONDS ) )
            {
                throw new IllegalStateException( "Failed to acquire locks" );
            }

            // then shutdown in given order works
            preShutdown.complete( null );
            afterShutdown.get( LONG_TIME, MILLISECONDS );
        }
        finally
        {
            afterShutdown.cancel( true );
            locksHolder.countDown();
            txExecutor.shutdownNow();
            shutdownExecutor.shutdownNow();
        }
    }
}
