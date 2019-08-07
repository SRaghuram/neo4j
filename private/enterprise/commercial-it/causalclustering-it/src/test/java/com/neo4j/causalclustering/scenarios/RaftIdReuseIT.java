/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterRule;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class RaftIdReuseIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            // increased to decrease likelihood of unnecessary leadership changes
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "2s" )
            .withNumberOfReadReplicas( 0 );
    private Cluster cluster;

    @Test
    public void shouldReuseIdsInCluster() throws Exception
    {
        cluster = clusterRule.startCluster();

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
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Node node3 = db.createNode();

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
    public void newLeaderShouldReuseIdsFreedOnPreviousLeader() throws Exception
    {
        cluster = clusterRule.startCluster();

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
            Node node = db.createNode();
            assertTrue( String.format("Created node had id %d, should be %d or %d", node.getId(), first.getValue(), second.getValue() ),
                    node.getId() == first.getValue() || node.getId() == second.getValue());

            tx.commit();
        } );
        assumeTrue( newLeader.equals( newCreationLeader ) );
    }

    @Test
    public void reusePreviouslyFreedIds() throws Exception
    {
        cluster = clusterRule.startCluster();

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
            Node node1 = db.createNode();
            Node node2 = db.createNode();

            node1id.setValue( node1.getId() );
            node2id.setValue( node2.getId() );

            tx.commit();
        } );
        assumeTrue( leader.equals( reuseLeader ) );

        assertEquals( first.longValue(), node1id.longValue() );
        assertEquals( second.longValue(), node2id.longValue() );
    }

    private void idMaintenanceOnLeader( CoreClusterMember leader )
    {
        IdController idController = resolveDependency( leader, IdController.class );
        idController.maintenance();
    }

    private <T> T resolveDependency( CoreClusterMember leader, Class<T> clazz )
    {
        return leader.defaultDatabase().getDependencyResolver().resolveDependency( clazz );
    }

    private CoreClusterMember removeTwoNodes( Cluster cluster, MutableLong first, MutableLong second ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.getNodeById( first.longValue() );
            node1.delete();

            db.getNodeById( second.longValue() ).delete();

            tx.commit();
        } );
    }

    private CoreClusterMember createThreeNodes( Cluster cluster, MutableLong first, MutableLong second ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node1 = db.createNode();
            first.setValue( node1.getId() );

            Node node2 = db.createNode();
            second.setValue( node2.getId() );

            db.createNode();

            tx.commit();
        } );
    }
}
