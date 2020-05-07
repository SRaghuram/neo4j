/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMonitors;
import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogCommitIndexMonitor;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdController;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Barrier;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.forceReelection;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.hamcrest.Matchers.equalTo;
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

    @BeforeEach
    void startExecutor()
    {
        executorService = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void stopExecutor()
    {
        executorService.shutdown();
    }

    @Test
    void shouldTryToReproduceIt() throws Exception
    {
        // given
        Cluster cluster = clusterFactory.createCluster( clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( CausalClusteringSettings.failure_detection_window, "2s-3s" )
                .withSharedCoreParam( CausalClusteringSettings.failure_resolution_window, "2s-3s" ) );
        cluster.start();
        //add monitors
        List<StubRaftLogCommitIndexMonitor> monitors = registerMonitors( cluster );

        // Instance A is leader
        CoreClusterMember instanceA = cluster.awaitLeader();
        Set<CoreClusterMember> allMembers = cluster.coreMembers();
        // Relationship R, created and in use
        long node = createNode( instanceA );
        long otherNode = createNode( instanceA );
        createRelationship( instanceA, node, null, null ); // <-- simply to sit there in the chain so that R locks it on createCommands
        long relationship = createRelationship( instanceA, node, null, null );
        long unrelatedRelationship = createRelationship( instanceA, otherNode, null, null );
        // Relationship R deleted, and therefore in the freelist of course
        deleteRelationship( instanceA, relationship );
        doIdMaintenance( allMembers );
        deleteRelationship( instanceA, unrelatedRelationship ); // <-- simply to sit there in the id generator on the same range

        // when
        // A transaction T that creates a relationship, which will be R (assert this)... DON'T COMMIT, BUT KEEP THE TX OPEN!
        Barrier.Control barrier = new Barrier.Control();
        Future<Long> t = executorService.submit( () -> createRelationship( instanceA, node, barrier::reached, id -> assertEquals( relationship, id ) ) );
        barrier.await();
        // Do a leader switch, so that instance B is now leader
        // Instance B performs a transaction that creates a relationship, which will also be R (assert this)
        CoreClusterMember instanceB = switchLeaderFrom( cluster, monitors, instanceA );
        doIdMaintenance( allMembers );
        createRelationship( instanceB, node, null, id -> assertEquals( relationship, id ) );
        // Now do a leader switch back to instance A
        switchLeaderTo( cluster, monitors, instanceA );
        // Let T continue and fail, which will then mark R as deleted when it's rolling back     <---- this is the bug, right there
        barrier.release();
        assertThrows( Exception.class, t::get );
        // Instance A creates a relationship, which will be R (assert this) for some other node
        doIdMaintenance( allMembers );

        // then
        for ( int i = 0; i < 10; i++ )
        {
            createRelationship( instanceA, otherNode, null, id -> assertNotEquals( relationship, id ) );
        }
    }

    private CoreClusterMember switchLeaderFrom( Cluster cluster,
            List<StubRaftLogCommitIndexMonitor> monitors,
            CoreClusterMember expectedCurrentLeader ) throws Exception
    {
        return switchLeader( cluster, monitors, memberId -> expectedCurrentLeader.id().equals( memberId ) );
    }

    private void switchLeaderTo( Cluster cluster,
            List<StubRaftLogCommitIndexMonitor> monitors,
            CoreClusterMember expectedNewLeader ) throws Exception
    {
        switchLeader( cluster, monitors, memberId -> !memberId.equals( expectedNewLeader.id() ) );
    }

    private void doIdMaintenance( Iterable<CoreClusterMember> members )
    {
        members.forEach( this::doIdMaintenance );
    }

    private void doIdMaintenance( CoreClusterMember instance )
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) instance.managementService().database( DEFAULT_DATABASE_NAME );
        db.getDependencyResolver().resolveDependency( IdController.class ).maintenance();
    }

    private CoreClusterMember switchLeader( Cluster cluster,
            List<StubRaftLogCommitIndexMonitor> monitors,
            Predicate<MemberId> shouldRetry ) throws Exception
    {
        forceReelection( cluster, DEFAULT_DATABASE_NAME );
        var newLeader = cluster.awaitLeader();
        if ( shouldRetry.test( newLeader.id() ) )
        {
            assertEventually( "Members could not catch up", () -> allMonitorsHaveSameIndex( monitors ), same -> same, 30, TimeUnit.SECONDS );
            return switchLeader( cluster, monitors, shouldRetry );
        }
        return newLeader;
    }

    private void deleteRelationship( CoreClusterMember instance, long relationship )
    {
        GraphDatabaseService db = instance.managementService().database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            tx.getRelationshipById( relationship ).delete();
            tx.commit();
        }
    }

    private long createRelationship( CoreClusterMember instance, long nodeId, Runnable waiter, LongConsumer relationshipIdVerifier )
    {
        GraphDatabaseService db = instance.managementService().database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            Relationship relationship = node.createRelationshipTo( node, MyRelTypes.TEST );
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
        GraphDatabaseService db = instance.managementService().database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            tx.commit();
            return node.getId();
        }
    }

    private List<StubRaftLogCommitIndexMonitor> registerMonitors( Cluster cluster )
    {
        List<StubRaftLogCommitIndexMonitor> monitors = new ArrayList<>();
        cluster.coreMembers().forEach( coreClusterMember -> {
            var monitor = new StubRaftLogCommitIndexMonitor();
            var clusterMonitors = coreClusterMember.defaultDatabase().getDependencyResolver().resolveDependency( ClusterMonitors.class );
            clusterMonitors.addMonitorListener( monitor );
            monitors.add( monitor );
        } );
        return monitors;
    }

    private boolean allMonitorsHaveSameIndex( List<StubRaftLogCommitIndexMonitor> monitors )
    {
        long maxCommitIndex = monitors.stream().map( StubRaftLogCommitIndexMonitor::commitIndex ).max(Long::compare).orElse( 0L );
        return monitors.stream().map( StubRaftLogCommitIndexMonitor::commitIndex ).allMatch( commitIndex -> commitIndex == maxCommitIndex );
    }

    private static class StubRaftLogCommitIndexMonitor implements RaftLogCommitIndexMonitor
    {
        private long commitIndex;

        @Override
        public long commitIndex()
        {
            return commitIndex;
        }

        @Override
        public void commitIndex( long commitIndex )
        {
            this.commitIndex = commitIndex;
        }
    }
}
