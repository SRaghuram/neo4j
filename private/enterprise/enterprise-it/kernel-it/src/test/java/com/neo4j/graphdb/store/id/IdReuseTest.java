/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb.store.id;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.id.IdController;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@EnterpriseDbmsExtension
class IdReuseTest
{
    @Inject
    private DatabaseManagementService dbms;
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private IdController idController;

    @Test
    void shouldReuseNodeIdsFromRolledBackTransaction()
    {
        // Given
        long rolledBackNodeId;
        try ( Transaction tx = db.beginTx() )
        {
            rolledBackNodeId = tx.createNode().getId();

            tx.rollback();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();

            tx.commit();
        }

        restartDatabase();

        // When
        long reusedNodeId;
        try ( Transaction tx = db.beginTx() )
        {
            reusedNodeId = tx.createNode().getId();

            tx.commit();
        }

        // Then
        assertThat( reusedNodeId ).isEqualTo( rolledBackNodeId );
    }

    @Test
    void shouldReuseRelationshipIdsFromRolledBackTransaction()
    {
        // Given
        Node node1;
        Node node2;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = tx.createNode();
            node2 = tx.createNode();

            tx.commit();
        }

        long rolledBackRelationshipId;
        try ( Transaction tx = db.beginTx() )
        {
            rolledBackRelationshipId = tx.getNodeById( node1.getId() )
                    .createRelationshipTo( tx.getNodeById( node2.getId() ), RelationshipType.withName( "LIKE" ) ).getId();

            tx.rollback();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node1.getId() )
                    .createRelationshipTo( tx.getNodeById( node2.getId() ), RelationshipType.withName( "LIKE" ) ).getId();

            tx.commit();
        }

        restartDatabase();

        // When
        long reusedRelationshipId;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = tx.getNodeById( node1.getId() );
            node2 = tx.getNodeById( node2.getId() );
            reusedRelationshipId = node1.createRelationshipTo( node2, RelationshipType.withName( "LIKE" ) ).getId();

            tx.commit();
        }

        // Then
        assertThat( reusedRelationshipId ).isEqualTo( rolledBackRelationshipId );
    }

    @Test
    void sequentialOperationRelationshipIdReuse()
    {
        Label marker = Label.label( "marker" );

        long relationship1 = createRelationship( marker );
        long relationship2 = createRelationship( marker );
        long relationship3 = createRelationship( marker );

        assertEquals( relationship1 + 1, relationship2, "Ids should be sequential" );
        assertEquals( relationship2 + 1, relationship3, "Ids should be sequential" );

        deleteRelationshipByLabelAndRelationshipType( marker );

        idController.maintenance( true );

        assertEquals( relationship1, createRelationship( marker ), "Relationships have reused id" );
        assertEquals( relationship2, createRelationship( marker ), "Relationships have reused id" );
        assertEquals( relationship3, createRelationship( marker ), "Relationships have reused id" );
    }

    @Test
    void relationshipIdReusableOnlyAfterTransactionFinish()
    {
        Label testLabel = Label.label( "testLabel" );
        long relationshipId = createRelationship( testLabel );

        try ( Transaction transaction = db.beginTx();
              ResourceIterator<Node> nodes = transaction.findNodes( testLabel ) )
        {
            List<Node> nodeList = Iterators.asList( nodes );
            for ( Node node : nodeList )
            {
                Iterable<Relationship> relationships = node.getRelationships( TestRelationshipType.MARKER );
                for ( Relationship relationship : relationships )
                {
                    relationship.delete();
                }
            }

            idController.maintenance( true );

            Node node1 = transaction.createNode( testLabel );
            Node node2 = transaction.createNode( testLabel );

            Relationship relationshipTo = node1.createRelationshipTo( node2, TestRelationshipType.MARKER );

            assertNotEquals( relationshipId, relationshipTo.getId(), "Relationships should have different ids." );
            transaction.commit();
        }
    }

    private void restartDatabase()
    {
        dbms.shutdownDatabase( db.databaseLayout().getDatabaseName() );
        dbms.startDatabase( db.databaseLayout().getDatabaseName() );
    }

    private void deleteRelationshipByLabelAndRelationshipType( Label marker )
    {
        try ( Transaction transaction = db.beginTx();
              ResourceIterator<Node> nodes = transaction.findNodes( marker ) )
        {
            List<Node> nodeList = Iterators.asList( nodes );
            for ( Node node : nodeList )
            {
                Iterable<Relationship> relationships = node.getRelationships( TestRelationshipType.MARKER );
                for ( Relationship relationship : relationships )
                {
                    relationship.delete();
                }
            }
            transaction.commit();
        }
    }

    private long createRelationship( Label label )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = transaction.createNode( label );
            Node node2 = transaction.createNode( label );

            Relationship relationshipTo = node1.createRelationshipTo( node2, TestRelationshipType.MARKER );
            transaction.commit();
            return relationshipTo.getId();
        }
    }

    private enum TestRelationshipType implements RelationshipType
    {
        MARKER
    }
}
