/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.jar.JarBuilder;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@TestDirectoryExtension
public class EagerProcedureIT
{
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @Test
    void shouldNotGetPropertyAccessFailureWhenStreamingToAnEagerDestructiveProcedure()
    {
        // When we have a simple graph (a)
        setUpTestData();

        try ( Transaction transaction = db.beginTx() )
        {
            // Then we can run an eagerized destructive procedure
            Result res = transaction.execute( "MATCH (n) WHERE n.key = 'value' " + "WITH n CALL com.neo4j.procedure.deleteNeighboursEagerized(n, 'FOLLOWS') " +
                    "YIELD value RETURN value" );
            assertThat( res.resultAsString() ).as( "Should get as many rows as original nodes" ).contains( "2 rows" );
            transaction.commit();
        }
    }

    @Test
    void shouldGetPropertyAccessFailureWhenStreamingToANonEagerDestructiveProcedure()
    {
        // When we have a simple graph (a)
        setUpTestData();

        try ( Transaction transaction = db.beginTx() )
        {
            // When we try to run an eagerized destructive procedure
            QueryExecutionException exception = assertThrows( QueryExecutionException.class, () -> transaction.execute(
                    "MATCH (n) WHERE n.key = 'value' " + "WITH n CALL com.neo4j.procedure.deleteNeighboursNotEagerized(n, 'FOLLOWS') " +
                            "YIELD value RETURN value" ) );
            assertThat( exception.getMessage() ).isEqualTo( "Node with id 1 has been deleted in this transaction" );
        }
    }

    @Test
    void shouldNotGetErrorBecauseOfNormalEagerizationWhenStreamingFromANormalReadProcedureToDestructiveCypher()
    {
        // When we have a simple graph (a)
        int count = 10;
        setUpTestData( count );

        try ( Transaction transaction = db.beginTx() )
        {
            // Then we can run an normal read procedure and it will be eagerized by normal Cypher eagerization
            Result res = transaction.execute(
                    "MATCH (n) WHERE n.key = 'value' " + "CALL com.neo4j.procedure.findNeighboursNotEagerized(n) " + "YIELD relationship AS r, node as m " +
                            "DELETE r, m RETURN true" );
            assertThat( res.resultAsString() ).as( "Should get one fewer rows than original nodes" ).contains( (count - 1) + " rows" );
            assertThat( res.getExecutionPlanDescription().toString() ).as( "The plan description should contain the 'Eager' operation" ).contains( "+Eager" );
            transaction.commit();
        }
    }

    @Test
    void shouldGetEagerPlanForAnEagerProcedure()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // When explaining a call to an eagerized procedure
            Result res = transaction.execute(
                    "EXPLAIN MATCH (n) WHERE n.key = 'value' " + "WITH n CALL com.neo4j.procedure.deleteNeighboursEagerized(n, 'FOLLOWS') " +
                            "YIELD value RETURN value" );
            assertThat( res.getExecutionPlanDescription().toString() ).as( "The plan description should contain the 'Eager' operation" ).contains( "+Eager" );
            transaction.commit();
        }
    }

    @Test
    void shouldNotGetEagerPlanForANonEagerProcedure()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // When explaining a call to an non-eagerized procedure
            Result res = transaction.execute(
                    "EXPLAIN MATCH (n) WHERE n.key = 'value' " + "WITH n CALL com.neo4j.procedure.deleteNeighboursNotEagerized(n, 'FOLLOWS') " +
                            "YIELD value RETURN value" );
            assertThat( res.getExecutionPlanDescription().toString() ).as( "The plan description shouldn't contain the 'Eager' operation" )
                    .doesNotContain( "+Eager" );
            transaction.commit();
        }
    }

    private void setUpTestData()
    {
        setUpTestData( 2 );
    }

    private void setUpTestData( int nodes )
    {
        try ( Transaction tx = db.beginTx() )
        {
            createChainOfNodesWithLabelAndProperty( tx, nodes, "FOLLOWS", "User", "key", "value" );
            tx.commit();
        }
    }

    private void createChainOfNodesWithLabelAndProperty( Transaction tx, int length, String relationshipName, String labelName, String property, String value )
    {
        RelationshipType relationshipType = RelationshipType.withName( relationshipName );
        Label label = Label.label( labelName );
        Node prev = null;
        for ( int i = 0; i < length; i++ )
        {
            Node node = tx.createNode( label );
            node.setProperty( property, value );
            if ( !property.equals( "name" ) )
            {
                node.setProperty( "name", labelName + " " + i );
            }
            if ( prev != null )
            {
                prev.createRelationshipTo( node, relationshipType );
            }
            prev = node;
        }
    }

    @BeforeEach
    void setUp() throws IOException
    {
        new JarBuilder().createJarFor( testDirectory.createFile( "myProcedures.jar" ), ClassWithProcedures.class );
        managementService = new TestDatabaseManagementServiceBuilder().impermanent()
                .setConfig( GraphDatabaseSettings.plugin_dir, testDirectory.absolutePath() ).build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        if ( this.db != null )
        {
            this.managementService.shutdown();
        }
    }

    public static class Output
    {
        public final long value;

        public Output( long value )
        {
            this.value = value;
        }
    }

    public static class NeighbourOutput
    {
        public final Relationship relationship;
        public final Node node;

        public NeighbourOutput( Relationship relationship, Node node )
        {
            this.relationship = relationship;
            this.node = node;
        }
    }

    @SuppressWarnings( "unused" )
    public static class ClassWithProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Procedure( mode = READ )
        public Stream<NeighbourOutput> findNeighboursNotEagerized( @Name( "node" ) Node node )
        {
            return findNeighbours( node );
        }

        private Stream<NeighbourOutput> findNeighbours( Node node )
        {
            return StreamSupport.stream( node.getRelationships( Direction.OUTGOING ).spliterator(), false ).map(
                    relationship -> new NeighbourOutput( relationship, relationship.getOtherNode( node ) ) );
        }

        @Procedure( mode = WRITE, eager = true )
        public Stream<Output> deleteNeighboursEagerized( @Name( "node" ) Node node, @Name( "relation" ) String relation )
        {
            return Stream.of( new Output( deleteNeighbours( node, RelationshipType.withName( relation ) ) ) );
        }

        @Procedure( mode = WRITE )
        public Stream<Output> deleteNeighboursNotEagerized( @Name( "node" ) Node node, @Name( "relation" ) String relation )
        {
            return Stream.of( new Output( deleteNeighbours( node, RelationshipType.withName( relation ) ) ) );
        }

        private long deleteNeighbours( Node node, RelationshipType relType )
        {
            try
            {
                long deleted = 0;
                for ( Relationship rel : node.getRelationships() )
                {
                    Node other = rel.getOtherNode( node );
                    rel.delete();
                    other.delete();
                    deleted++;
                }
                return deleted;
            }
            catch ( NotFoundException e )
            {
                // Procedures should internally handle missing nodes due to lazy interactions
                return 0;
            }
        }
    }
}
