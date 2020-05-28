/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.neo4j.graphdb.Node;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@ClusterExtension
@TestInstance( PER_METHOD )
class RaftIdReuseIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void beforeEach() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                // increased to decrease likelihood of unnecessary leadership changes
                .withSharedCoreParam( CausalClusteringSettings.leader_failure_detection_window, "2s-3s" )
                .withSharedCoreParam( CausalClusteringSettings.election_failure_detection_window, "2s-3s" )
                .withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldReuseIdsInCluster() throws Exception
    {
        final MutableLong first = new MutableLong();
        final MutableLong second = new MutableLong();

        CoreClusterMember leader1 = createThreeNodes( cluster, first, second );
        CoreClusterMember leader2 = removeTwoNodes( cluster, first, second );

        assumeTrue( leader1 != null && leader1.equals( leader2 ) );

        // Force maintenance on leader
        idMaintenanceOnLeader( leader1 );
        IdGeneratorFactory idGeneratorFactory = resolveDependency( leader1, IdGeneratorFactory.class );
        final IdGenerator idGenerator = idGeneratorFactory.get( IdType.NODE );
        assertEquals( 2, idGenerator.getDefragCount() );

        final MutableLong node1id = new MutableLong();
        final MutableLong node2id = new MutableLong();
        final MutableLong node3id = new MutableLong();

        CoreClusterMember clusterMember = cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            Node node3 = tx.createNode();

            node1id.setValue( node1.getId() );
            node2id.setValue( node2.getId() );
            node3id.setValue( node3.getId() );

            tx.commit();
        } );

        assumeTrue( leader1.equals( clusterMember ) );

        assertEquals( first.longValue(), node1id.longValue() );
        assertEquals( second.longValue(), node2id.longValue() );
        assertEquals( idGenerator.getHighestPossibleIdInUse(), node3id.longValue() );
    }

    @Test
    void newLeaderShouldReuseIdsFreedOnPreviousLeader() throws Exception
    {
        final MutableLong first = new MutableLong();
        final MutableLong second = new MutableLong();

        // Create three, delete two nodes on the leader. The leader should report two defragged ids
        CoreClusterMember creationLeader = createThreeNodes( cluster, first, second );
        CoreClusterMember deletionLeader = removeTwoNodes( cluster, first, second );

        // the following assumption is not sufficient for the subsequent assertions, since leadership is a volatile state
        assumeTrue( creationLeader != null && creationLeader.equals( deletionLeader ) );

        idMaintenanceOnLeader( creationLeader );
        IdGeneratorFactory idGeneratorFactory = resolveDependency( creationLeader, IdGeneratorFactory.class );
        IdGenerator creationLeaderIdGenerator = idGeneratorFactory.get( IdType.NODE );
        assertEquals( 2, creationLeaderIdGenerator.getDefragCount() );

        // Force leader switch
        cluster.removeCoreMemberWithServerId( creationLeader.serverId() );

        // waiting for new leader
        CoreClusterMember newLeader = cluster.awaitLeader();
        assertNotSame( creationLeader.serverId(), newLeader.serverId() );
        idMaintenanceOnLeader( newLeader );

        // The new leader should still have 2 defragged ids
        IdGeneratorFactory newLeaderIdGeneratorFactory = resolveDependency( newLeader, IdGeneratorFactory.class );
        final IdGenerator idGenerator = newLeaderIdGeneratorFactory.get( IdType.NODE );
        assertEquals( 2, idGenerator.getDefragCount() );

        // The ids available should be actually reused, so check that a new node gets one of the above deleted ids
        CoreClusterMember newCreationLeader = cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode();
            assertTrue( node.getId() == first.getValue() || node.getId() == second.getValue(),
                    String.format( "Created node had id %d, should be %d or %d", node.getId(), first.getValue(), second.getValue() ) );

            tx.commit();
        } );
        assumeTrue( newLeader.equals( newCreationLeader ) );
    }

    @Test
    void reusePreviouslyFreedIds() throws Exception
    {
        final MutableLong first = new MutableLong();
        final MutableLong second = new MutableLong();

        CoreClusterMember creationLeader = createThreeNodes( cluster, first, second );
        CoreClusterMember deletionLeader = removeTwoNodes( cluster, first, second );

        assumeTrue( creationLeader != null && creationLeader.equals( deletionLeader ) );
        IdGeneratorFactory idGeneratorFactory = resolveDependency( creationLeader, IdGeneratorFactory.class );
        idMaintenanceOnLeader( creationLeader );
        IdGenerator creationLeaderIdGenerator = idGeneratorFactory.get( IdType.NODE );
        assertEquals( 2, creationLeaderIdGenerator.getDefragCount() );

        // Restart and re-elect first leader
        cluster.removeCoreMemberWithServerId( creationLeader.serverId() );
        cluster.addCoreMemberWithId( creationLeader.serverId() ).start();

        CoreClusterMember leader = cluster.awaitLeader();
        while ( leader.serverId() != creationLeader.serverId() )
        {
            cluster.removeCoreMemberWithServerId( leader.serverId() );
            cluster.addCoreMemberWithId( leader.serverId() ).start();
            leader = cluster.awaitLeader();
        }

        idMaintenanceOnLeader( leader );
        IdGeneratorFactory leaderIdGeneratorFactory = resolveDependency( leader, IdGeneratorFactory.class );
        creationLeaderIdGenerator = leaderIdGeneratorFactory.get( IdType.NODE );
        assertEquals( 2, creationLeaderIdGenerator.getDefragCount() );

        final MutableLong node1id = new MutableLong();
        final MutableLong node2id = new MutableLong();
        CoreClusterMember reuseLeader = cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();

            node1id.setValue( node1.getId() );
            node2id.setValue( node2.getId() );

            tx.commit();
        } );
        assumeTrue( leader.equals( reuseLeader ) );

        assertEquals( first.longValue(), node1id.longValue() );
        assertEquals( second.longValue(), node2id.longValue() );
    }

    private static void idMaintenanceOnLeader( CoreClusterMember leader )
    {
        IdController idController = resolveDependency( leader, IdController.class );
        idController.maintenance();
    }

    private static <T> T resolveDependency( CoreClusterMember leader, Class<T> clazz )
    {
        return leader.defaultDatabase().getDependencyResolver().resolveDependency( clazz );
    }

    private static CoreClusterMember removeTwoNodes( Cluster cluster, MutableLong first, MutableLong second ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = tx.getNodeById( first.longValue() );
            node1.delete();

            tx.getNodeById( second.longValue() ).delete();

            tx.commit();
        } );
    }

    private static CoreClusterMember createThreeNodes( Cluster cluster, MutableLong first, MutableLong second ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = tx.createNode();
            first.setValue( node1.getId() );

            Node node2 = tx.createNode();
            second.setValue( node2.getId() );

            tx.createNode();

            tx.commit();
        } );
    }
}
