/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
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
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.jar.JarBuilder;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@ExtendWith( TestDirectoryExtension.class )
public class EagerProcedureIT
{
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService db;

    @Test
    void shouldNotGetPropertyAccessFailureWhenStreamingToAnEagerDestructiveProcedure()
    {
        // When we have a simple graph (a)
        setUpTestData();

        // Then we can run an eagerized destructive procedure
        Result res = db.execute( "MATCH (n) WHERE n.key = 'value' " +
                "WITH n CALL org.neo4j.procedure.deleteNeighboursEagerized(n, 'FOLLOWS') " +
                "YIELD value RETURN value" );
        assertThat( "Should get as many rows as original nodes", res.resultAsString(), containsString( "2 rows" ) );
    }

    @Test
    void shouldGetPropertyAccessFailureWhenStreamingToANonEagerDestructiveProcedure()
    {
        // When we have a simple graph (a)
        setUpTestData();

        // When we try to run an eagerized destructive procedure
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () -> db.execute( "MATCH (n) WHERE n.key = 'value' " +
                "WITH n CALL org.neo4j.procedure.deleteNeighboursNotEagerized(n, 'FOLLOWS') " +
                "YIELD value RETURN value" ) );
        assertThat( exception.getMessage(), equalTo( "Node with id 1 has been deleted in this transaction" ) );
    }

    @Test
    void shouldNotGetErrorBecauseOfNormalEagerizationWhenStreamingFromANormalReadProcedureToDestructiveCypher()
    {
        // When we have a simple graph (a)
        int count = 10;
        setUpTestData( count );

        // Then we can run an normal read procedure and it will be eagerized by normal Cypher eagerization
        Result res = db.execute( "MATCH (n) WHERE n.key = 'value' " +
                "CALL org.neo4j.procedure.findNeighboursNotEagerized(n) " +
                "YIELD relationship AS r, node as m " +
                "DELETE r, m RETURN true" );
        assertThat( "Should get one fewer rows than original nodes", res.resultAsString(), containsString( (count - 1) + " rows" ) );
        assertThat( "The plan description should contain the 'Eager' operation", res.getExecutionPlanDescription().toString(), containsString( "+Eager" ) );
    }

    @Test
    void shouldGetEagerPlanForAnEagerProcedure()
    {
        // When explaining a call to an eagerized procedure
        Result res = db.execute( "EXPLAIN MATCH (n) WHERE n.key = 'value' " +
                "WITH n CALL org.neo4j.procedure.deleteNeighboursEagerized(n, 'FOLLOWS') " +
                "YIELD value RETURN value" );
        assertThat( "The plan description should contain the 'Eager' operation", res.getExecutionPlanDescription().toString(), containsString( "+Eager" ) );
    }

    @Test
    void shouldNotGetEagerPlanForANonEagerProcedure()
    {
        // When explaining a call to an non-eagerized procedure
        Result res = db.execute( "EXPLAIN MATCH (n) WHERE n.key = 'value' " +
                "WITH n CALL org.neo4j.procedure.deleteNeighboursNotEagerized(n, 'FOLLOWS') " +
                "YIELD value RETURN value" );
        assertThat( "The plan description shouldn't contain the 'Eager' operation", res.getExecutionPlanDescription().toString(),
                Matchers.not( containsString( "+Eager" ) ) );
    }

    private void setUpTestData()
    {
        setUpTestData( 2 );
    }

    private void setUpTestData( int nodes )
    {
        try ( Transaction tx = db.beginTx() )
        {
            createChainOfNodesWithLabelAndProperty( nodes, "FOLLOWS", "User", "key", "value" );
            tx.success();
        }
    }

    private void createChainOfNodesWithLabelAndProperty( int length, String relationshipName, String labelName, String property, String value )
    {
        RelationshipType relationshipType = RelationshipType.withName( relationshipName );
        Label label = Label.label( labelName );
        Node prev = null;
        for ( int i = 0; i < length; i++ )
        {
            Node node = db.createNode( label );
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
        DatabaseManagementService managementService = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.plugin_dir, testDirectory.directory().getAbsolutePath() ).newDatabaseManagementService();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        if ( this.db != null )
        {
            this.db.shutdown();
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
