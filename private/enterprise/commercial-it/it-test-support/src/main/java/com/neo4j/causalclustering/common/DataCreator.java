/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.CoreClusterMember;

import java.util.UUID;
import java.util.function.Supplier;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;

public class DataCreator
{
    public static final Label LABEL = label( "ExampleNode" );
    public static final String NODE_PROPERTY_1 = "id";
    public static final String NODE_PROPERTY_2 = "name";
    public static final String RELATIONSHIP_PROPERTY = "id";
    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName( "LIKES" );

    public static final String NODE_PROPERTY_1_PREFIX = "foo-";

    private DataCreator()
    {
    }

    public static void createDataInMultipleTransactions( Cluster cluster, int size ) throws Exception
    {
        createLabelledNodesWithProperty( cluster, size, LABEL, () -> Pair.of( NODE_PROPERTY_1, UUID.randomUUID().toString() ) );
    }

    public static CoreClusterMember createDataInOneTransaction( Cluster cluster, int size ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            for ( int i = 0; i < size; i++ )
            {
                Node node1 = db.createNode( LABEL );
                Node node2 = db.createNode( LABEL );

                node1.setProperty( NODE_PROPERTY_1, nodeProperty1Value() );
                node1.setProperty( NODE_PROPERTY_2, "node1" );
                node2.setProperty( NODE_PROPERTY_1, nodeProperty1Value() );
                node2.setProperty( NODE_PROPERTY_2, "node2" );

                Relationship rel = node1.createRelationshipTo( node2, RELATIONSHIP_TYPE );
                rel.setProperty( RELATIONSHIP_PROPERTY, UUID.randomUUID().toString() );
                tx.success();
            }
        } );
    }

    public static CoreClusterMember createLabelledNodesWithProperty( Cluster cluster, int numberOfNodes,
            Label label, Supplier<Pair<String,Object>> propertyPair ) throws Exception
    {
        CoreClusterMember last = null;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            last = cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( label );
                node.setProperty( propertyPair.get().first(), propertyPair.get().other() );
                tx.success();
            } );
        }
        return last;
    }

    public static CoreClusterMember createEmptyNodes( Cluster cluster, int numberOfNodes ) throws Exception
    {
        CoreClusterMember last = null;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            last = cluster.coreTx( ( db, tx ) ->
            {
                db.createNode();
                tx.success();
            } );
        }
        return last;
    }

    public static long countNodes( CoreClusterMember member )
    {
        GraphDatabaseFacade db = member.defaultDatabase();
        long count;
        try ( Transaction tx = db.beginTx() )
        {
            count = count( db.getAllNodes() );
            tx.success();
        }
        return count;
    }

    public static void createSchema( Cluster cluster ) throws Exception
    {
        cluster.coreTx( ( db, tx ) ->
        {
            db.schema().constraintFor( LABEL ).assertPropertyIsUnique( NODE_PROPERTY_1 ).create();
            tx.success();
        } );
    }

    private static String nodeProperty1Value()
    {
        return NODE_PROPERTY_1_PREFIX + "-" + UUID.randomUUID();
    }
}
