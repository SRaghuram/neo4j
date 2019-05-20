/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb.store.id;

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import com.neo4j.test.extension.CommercialDbmsExtension;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@CommercialDbmsExtension( configurationCallback = "configure" )
class IdReuseTest
{
    @Inject
    private DatabaseManagementService dbms;
    @Inject
    private GraphDatabaseAPI db;

    @ExtensionCallback
    static void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( CommercialEditionSettings.idTypesToReuse, IdType.NODE + "," + IdType.RELATIONSHIP );
        builder.setConfig( GraphDatabaseSettings.record_id_batch_size, "1" );
    }

    @Test
    void shouldReuseNodeIdsFromRolledBackTransaction() throws Exception
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();

            tx.failure();
        }

        restartDatabase();

        // When
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();

            tx.success();
        }

        // Then
        assertThat( node.getId(), equalTo( 0L ) );
    }

    @Test
    void shouldReuseRelationshipIdsFromRolledBackTransaction() throws Exception
    {
        // Given
        Node node1;
        Node node2;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.createNode();
            node2 = db.createNode();

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node1.createRelationshipTo( node2, RelationshipType.withName( "LIKE" ) );

            tx.failure();
        }

        restartDatabase();

        // When
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.getNodeById( node1.getId() );
            node2 = db.getNodeById( node2.getId() );
            relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "LIKE" ) );

            tx.success();
        }

        // Then
        assertThat( relationship.getId(), equalTo( 0L ) );
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

        final IdController idMaintenanceController = getIdMaintenanceController();

        deleteRelationshipByLabelAndRelationshipType( marker );

        idMaintenanceController.maintenance();

        assertEquals( relationship1, createRelationship( marker ), "Relationships have reused id" );
        assertEquals( relationship2, createRelationship( marker ), "Relationships have reused id" );
        assertEquals( relationship3, createRelationship( marker ), "Relationships have reused id" );
    }

    @Test
    void relationshipIdReusableOnlyAfterTransactionFinish()
    {
        Label testLabel = Label.label( "testLabel" );
        long relationshipId = createRelationship( testLabel );

        final IdController idMaintenanceController = getIdMaintenanceController();

        try ( Transaction transaction = db.beginTx();
              ResourceIterator<Node> nodes = db.findNodes( testLabel ) )
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

            idMaintenanceController.maintenance();

            Node node1 = db.createNode( testLabel );
            Node node2 = db.createNode( testLabel );

            Relationship relationshipTo = node1.createRelationshipTo( node2, TestRelationshipType.MARKER );

            assertNotEquals( relationshipId, relationshipTo.getId(), "Relationships should have different ids." );
            transaction.success();
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
              ResourceIterator<Node> nodes = db.findNodes( marker ) )
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
            transaction.success();
        }
    }

    private IdController getIdMaintenanceController()
    {
        return db.getDependencyResolver().resolveDependency( IdController.class );
    }

    private long createRelationship( Label label )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode( label );
            Node node2 = db.createNode( label );

            Relationship relationshipTo = node1.createRelationshipTo( node2, TestRelationshipType.MARKER );
            transaction.success();
            return relationshipTo.getId();
        }
    }

    private enum TestRelationshipType implements RelationshipType
    {
        MARKER
    }
}
