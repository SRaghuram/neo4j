/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.RaftMonitors;
import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogCommitIndexMonitor;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import org.neo4j.internal.id.IdController;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Barrier;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.switchLeaderTo;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

/**
 * <ol>
 * <li>Instance A (leader) starts a transaction T and creates relationship R from a reused ID</li>
 * <li>Leader switch occurs and instance B becomes leader</li>
 * <li>Instance B starts and commits a transaction S and creates relationship R, which is the same ID as instance A used in its
 *   still currently open transaction</li>
 * <li>Leader switch occurs and instance A becomes leader again and now has got applied S</li>
 * <li>T goes into commit and notices that it cannot commit so decides to roll back. Doing so it would mark R as deleted and free</li>
 * <li>Instance A starts and commits a transaction that creates a relationship with the same ID as R has, but for something completely different</li>
 * </ol>
 *
 * The test is about ensuring that the relationship ID R isn't reused, i.e. marked as deleted and free in the T transaction.
 */
@ClusterExtension
class CCIdReuseBySwitchAndRollbackIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private ExecutorService executorService;
    private Cluster cluster;

    @BeforeEach
    void start() throws ExecutionException, InterruptedException
    {
        executorService = Executors.newSingleThreadExecutor();
        cluster = clusterFactory.createCluster( clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( CausalClusteringSettings.leader_balancing, "NO_BALANCING" ) );
        cluster.start();
    }

    @AfterEach
    void stop()
    {
        executorService.shutdown();
        cluster.shutdown();
    }

    @Test
    void shouldTryToReproduceIt() throws Exception
    {
        //add monitors
        var monitors = registerMonitors( cluster );

        // Instance A is leader
        var instanceA = cluster.awaitLeader();
        var allMembers = cluster.coreMembers();
        // Relationship R, created and in use
        var node = createNode( instanceA );
        var otherNode = createNode( instanceA );
        createRelationship( instanceA, node, null, null ); // <-- simply to sit there in the chain so that R locks it on createCommands
        var relationship = createRelationship( instanceA, node, null, null );
        var unrelatedRelationship = createRelationship( instanceA, otherNode, null, null );
        // Relationship R deleted, and therefore in the freelist of course
        deleteRelationship( instanceA, relationship );
        doIdMaintenance( allMembers );
        deleteRelationship( instanceA, unrelatedRelationship ); // <-- simply to sit there in the id generator on the same range

        // when
        // A transaction T that creates a relationship, which will be R (assert this)... DON'T COMMIT, BUT KEEP THE TX OPEN!
        var barrier = new Barrier.Control();
        var t = executorService.submit( () -> createRelationship( instanceA, node, barrier::reached, id -> assertEquals( relationship, id ) ) );
        barrier.await();
        // Do a leader switch, so that instance B is now leader
        // Instance B performs a transaction that creates a relationship, which will also be R (assert this)
        var instanceB = updateAndSwitchLeader( cluster, monitors );
        doIdMaintenance( allMembers );
        createRelationship( instanceB, node, null, id -> assertEquals( relationship, id ) );
        // Now do a leader switch back to instance A
        updateAndSwitchLeaderTo( cluster, monitors, instanceA );
        // Let T continue and fail, which will then mark R as deleted when it's rolling back     <---- this is the bug, right there
        barrier.release();
        assertThrows( Exception.class, t::get );
        // Instance A creates a relationship, which will be R (assert this) for some other node
        doIdMaintenance( allMembers );

        // then
        for ( var i = 0; i < 10; i++ )
        {
            createRelationship( instanceA, otherNode, null, id -> assertNotEquals( relationship, id ) );
        }
    }

    private void doIdMaintenance( Iterable<CoreClusterMember> members )
    {
        members.forEach( this::doIdMaintenance );
    }

    private void doIdMaintenance( CoreClusterMember instance )
    {
        var db = (GraphDatabaseAPI) instance.managementService().database( DEFAULT_DATABASE_NAME );
        db.getDependencyResolver().resolveDependency( IdController.class ).maintenance( true );
    }

    private CoreClusterMember updateAndSwitchLeader( Cluster cluster,
            List<RaftLogCommitIndexMonitor> monitors ) throws Exception
    {
        assertEventually( "Members could not catch up", () -> allMonitorsHaveSameIndex( monitors ), same -> same, 30, TimeUnit.SECONDS );
        return CausalClusteringTestHelpers.switchLeader( cluster );
    }

    private void updateAndSwitchLeaderTo( Cluster cluster,
            List<RaftLogCommitIndexMonitor> monitors,
            CoreClusterMember expectedNewLeader ) throws Exception
    {
        assertEventually( "Members could not catch up", () -> allMonitorsHaveSameIndex( monitors ), same -> same, 30, TimeUnit.SECONDS );
        switchLeaderTo( cluster, expectedNewLeader );
    }

    private void deleteRelationship( CoreClusterMember instance, long relationship )
    {
        var db = instance.managementService().database( DEFAULT_DATABASE_NAME );
        try ( var tx = db.beginTx() )
        {
            tx.getRelationshipById( relationship ).delete();
            tx.commit();
        }
    }

    private long createRelationship( CoreClusterMember instance, long nodeId, Runnable waiter, LongConsumer relationshipIdVerifier )
    {
        var db = instance.managementService().database( DEFAULT_DATABASE_NAME );
        try ( var tx = db.beginTx() )
        {
            var node = tx.getNodeById( nodeId );
            var relationship = node.createRelationshipTo( node, MyRelTypes.TEST );
            if ( relationshipIdVerifier != null )
            {
                relationshipIdVerifier.accept( relationship.getId() );
            }
            if ( waiter != null )
            {
                waiter.run();
            }
            tx.commit();
            return relationship.getId();
        }
    }

    private long createNode( CoreClusterMember instance )
    {
        var db = instance.managementService().database( DEFAULT_DATABASE_NAME );
        try ( var tx = db.beginTx() )
        {
            var node = tx.createNode();
            tx.commit();
            return node.getId();
        }
    }

    private static List<RaftLogCommitIndexMonitor> registerMonitors( Cluster cluster )
    {
        var monitors = new ArrayList<RaftLogCommitIndexMonitor>();
        cluster.coreMembers().forEach( coreClusterMember -> {
            var monitor = new StubRaftLogCommitIndexMonitor();
            var clusterMonitors = coreClusterMember.defaultDatabase().getDependencyResolver().resolveDependency( RaftMonitors.class );
            clusterMonitors.addMonitorListener( monitor );
            monitors.add( monitor );
        } );
        return monitors;
    }

    private static Long START = -1L;
    private static Long EXIT = -2L;

    private static boolean allMonitorsHaveSameIndex( List<RaftLogCommitIndexMonitor> monitors )
    {
        return !EXIT.equals( monitors.stream().map( RaftLogCommitIndexMonitor::commitIndex ).reduce( START,
                ( previous, actual ) -> !previous.equals( EXIT ) && ((previous.equals( START ) || previous.equals( actual ))) ? actual : EXIT ) );
    }

    private static class StubRaftLogCommitIndexMonitor implements RaftLogCommitIndexMonitor
    {
        private AtomicLong commitIndex = new AtomicLong( 0 );

        @Override
        public long commitIndex()
        {
            return commitIndex.get();
        }

        @Override
        public void commitIndex( long commitIndex )
        {
            this.commitIndex.set( commitIndex );
        }
    }
}
