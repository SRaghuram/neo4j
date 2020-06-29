/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.dbms.commandline.StoreCopyCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.commandline.dbms.StoreCopyCommandIT.getCopyName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class StoreCopyCommandFilterIT extends AbstractCommandIT
{
    private static final Label PERSON_LABEL = Label.label( "Person" );
    private static final Label TEAM_LABEL = Label.label( "Team" );
    private static final Label DIVISION_LABEL = Label.label( "Division" );
    private static final Label S1 = Label.label( "S1" );
    private static final Label S2 = Label.label( "S2" );
    private static final Label S3 = Label.label( "S3" );
    private static final Label SAll = Label.label( "SAll" );
    private static final RelationshipType MEMBER = RelationshipType.withName( "MEMBER" );
    private static final Comparator<Node> NODE_NAME_COMPARATOR = Comparator.comparing( node -> (String) node.getProperty( "name" ) );

    @Override
    @BeforeEach
    void setUp() throws java.io.IOException
    {
        super.setUp();
        createOriginalGraph();
    }

    @Test
    void selectSingleNodeLabel() throws Exception
    {
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--keep-only-nodes-with-labels=S3",
                "--skip-labels=S1,S2,S3,SAll"
        );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );

        //(p3 :Person {id:125, name: “Cat”, age: 54})
        //(p4 :Person {id:126, name: “Dan”})
        try ( Transaction tx = copyDb.beginTx() )
        {
            ResourceIterable<Node> allNodes = tx.getAllNodes();
            List<Node> nodes = allNodes.stream().collect( Collectors.toList() );
            assertThat( nodes.size() ).isEqualTo( 2 );
            nodes.sort( NODE_NAME_COMPARATOR );

            Node p3 = nodes.get( 0 );
            assertThat( p3.getLabels() ).containsOnly( PERSON_LABEL );
            assertThat( p3.getAllProperties() ).containsOnly( entry( "id", 125 ), entry( "name", "Cat" ), entry( "age", 54 ) );
            assertThat( p3.getRelationships() ).isEmpty();

            Node p4 = nodes.get( 1 );
            assertThat( p4.getLabels() ).containsOnly( PERSON_LABEL );
            assertThat( p4.getAllProperties() ).containsOnly( entry( "id", 126 ), entry( "name", "Dan" ) );
            assertThat( p4.getRelationships() ).isEmpty();

            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void selectMultipleNodeLabels() throws Exception
    {
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--keep-only-nodes-with-labels=S3,SAll",
                "--skip-labels=S1,S2,S3,SAll"
        );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );

        // (p3 :Person {id:125, name: “Cat”, age: 54}) -[:MEMBER]->(t1 :Team {name: “Foo”})
        // (p4 :Person {id:126, name: “Dan”}) -[:MEMBER]->(t2 :Team {name: “Bar”})
        //
        // (d1 :Division {name: “Marketing”})
        try ( Transaction tx = copyDb.beginTx() )
        {
            ResourceIterable<Node> allNodes = tx.getAllNodes();
            List<Node> nodes = allNodes.stream().sorted( NODE_NAME_COMPARATOR ).collect( Collectors.toList() );
            assertThat( nodes.size() ).isEqualTo( 5 );

            Node t2 = nodes.get( 0 );
            Node p3 = nodes.get( 1 );
            Node p4 = nodes.get( 2 );
            Node t1 = nodes.get( 3 );
            Node d1 = nodes.get( 4 );

            assertThat( p3.getLabels() ).containsOnly( PERSON_LABEL );
            assertThat( p3.getAllProperties() ).containsOnly( entry( "id", 125 ), entry( "name", "Cat" ), entry( "age", 54 ) );
            Iterator<Relationship> p3Relationships = p3.getRelationships().iterator();
            Relationship member = p3Relationships.next();
            assertThat( member.getEndNodeId() ).isEqualTo( t1.getId() );
            assertThat( member.getType() ).isEqualTo( MEMBER );
            assertThat( member.getAllProperties() ).isEmpty();
            assertFalse( p3Relationships.hasNext() );

            assertThat( p4.getLabels() ).containsOnly( PERSON_LABEL );
            assertThat( p4.getAllProperties() ).containsOnly( entry( "id", 126 ), entry( "name", "Dan" ) );
            Iterator<Relationship> p4Relationships = p4.getRelationships().iterator();
            member = p4Relationships.next();
            assertThat( member.getEndNodeId() ).isEqualTo( t2.getId() );
            assertThat( member.getType() ).isEqualTo( MEMBER );
            assertThat( member.getAllProperties() ).isEmpty();
            assertFalse( p4Relationships.hasNext() );

            assertThat( t1.getLabels() ).containsOnly( TEAM_LABEL );
            assertThat( t1.getAllProperties() ).containsOnly( entry( "name", "Foo" ) );
            assertThat( t1.getRelationships() ).hasSize( 1 );

            assertThat( t2.getLabels() ).containsOnly( TEAM_LABEL );
            assertThat( t2.getAllProperties() ).containsOnly( entry( "name", "Bar" ) );
            assertThat( t2.getRelationships() ).hasSize( 1 );

            assertThat( d1.getLabels() ).containsOnly( DIVISION_LABEL );
            assertThat( d1.getAllProperties() ).containsOnly( entry( "name", "Marketing" ) );
            assertThat( d1.getRelationships() ).isEmpty();

            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void dropPropertiesByLabel() throws Exception
    {
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--keep-only-nodes-with-labels=S3,SAll",
                "--skip-labels=S1,S2,S3,SAll",
                "--skip-node-properties=Person.name,Person.age"
        );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );

        // (p3 :Person {id:125} )-[:MEMBER]->(t1 :Team {name: “Foo”})
        // (p4 :Person {id:126} )-[:MEMBER]->(t2 :Team {name: “Bar”})
        //
        // (d1 :Division {name: “Marketing”})
        try ( Transaction tx = copyDb.beginTx() )
        {
            ResourceIterable<Node> allNodes = tx.getAllNodes();
            List<Node> nodes = allNodes.stream().collect( Collectors.toList() );
            assertThat( nodes.size() ).isEqualTo( 5 );

            List<Node> teamNodes = nodes.stream().filter( node -> node.hasLabel( TEAM_LABEL ) ).sorted( NODE_NAME_COMPARATOR ).collect( Collectors.toList() );
            assertThat( teamNodes ).hasSize( 2 );
            Node t2 = teamNodes.get( 0 );
            Node t1 = teamNodes.get( 1 );

            assertThat( t1.getLabels() ).containsOnly( TEAM_LABEL );
            assertThat( t1.getAllProperties() ).containsOnly( entry( "name", "Foo" ) );
            Iterator<Relationship> relationships = t1.getRelationships().iterator();
            Relationship member = relationships.next();
            assertThat( member.getType() ).isEqualTo( MEMBER );
            assertThat( member.getAllProperties() ).isEmpty();

            Node p3 = member.getStartNode();
            assertFalse( relationships.hasNext() );
            assertThat( p3.getLabels() ).containsOnly( PERSON_LABEL );
            assertThat( p3.getAllProperties() ).containsOnly( entry( "id", 125 ) );
            assertThat( p3.getRelationships() ).hasSize( 1 );

            assertThat( t2.getLabels() ).containsOnly( TEAM_LABEL );
            assertThat( t2.getAllProperties() ).containsOnly( entry( "name", "Bar" ) );
            relationships = t2.getRelationships().iterator();
            member = relationships.next();
            assertThat( member.getType() ).isEqualTo( MEMBER );
            assertThat( member.getAllProperties() ).isEmpty();

            Node p4 = member.getStartNode();
            assertFalse( relationships.hasNext() );
            assertThat( p4.getLabels() ).containsOnly( PERSON_LABEL );
            assertThat( p4.getAllProperties() ).containsOnly( entry( "id", 126 ) );
            assertThat( p4.getRelationships() ).hasSize( 1 );

            List<Node> division = nodes.stream().filter( node -> node.hasLabel( DIVISION_LABEL ) ).collect( Collectors.toList() );
            assertThat( division ).hasSize( 1 );
            Node d1 = division.get( 0 );
            assertThat( d1.getLabels() ).containsOnly( DIVISION_LABEL );
            assertThat( d1.getAllProperties() ).containsOnly( entry( "name", "Marketing" ) );
            assertThat( d1.getRelationships() ).isEmpty();

            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void dropPropertiesByLabelWithoutNeedingToMentionAll() throws Exception
    {
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--keep-only-nodes-with-labels=S3,SAll",
                "--skip-labels=S1,S2,S3,SAll",
                "--keep-only-node-properties=Person.id,Division.name,Team.name"
        );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );

        //(p3 :Person {id:125}) -[:MEMBER]->(t1 :Team {name: “Foo”})
        //(p4 :Person {id:126}) -[:MEMBER]->(t2 :Team {name: “Bar”})
        //
        //(d1 :Division {name: “Marketing”})
        try ( Transaction tx = copyDb.beginTx() )
        {
            ResourceIterable<Node> allNodes = tx.getAllNodes();
            List<Node> nodes = allNodes.stream().collect( Collectors.toList() );
            assertThat( nodes.size() ).isEqualTo( 5 );

            List<Node> teamNodes = nodes.stream().filter( node -> node.hasLabel( TEAM_LABEL ) ).sorted( NODE_NAME_COMPARATOR ).collect( Collectors.toList() );
            assertThat( teamNodes ).hasSize( 2 );
            Node t2 = teamNodes.get( 0 );
            Node t1 = teamNodes.get( 1 );

            assertThat( t1.getLabels() ).containsOnly( TEAM_LABEL );
            assertThat( t1.getAllProperties() ).containsOnly( entry( "name", "Foo" ) );
            Iterator<Relationship> relationships = t1.getRelationships().iterator();
            Relationship member = relationships.next();
            assertThat( member.getType() ).isEqualTo( MEMBER );
            assertThat( member.getAllProperties() ).isEmpty();

            Node p3 = member.getStartNode();
            assertFalse( relationships.hasNext() );
            assertThat( p3.getLabels() ).containsOnly( PERSON_LABEL );
            assertThat( p3.getAllProperties() ).containsOnly( entry( "id", 125 ) );
            assertThat( p3.getRelationships() ).hasSize( 1 );

            assertThat( t2.getLabels() ).containsOnly( TEAM_LABEL );
            assertThat( t2.getAllProperties() ).containsOnly( entry( "name", "Bar" ) );
            relationships = t2.getRelationships().iterator();
            member = relationships.next();
            assertThat( member.getType() ).isEqualTo( MEMBER );
            assertThat( member.getAllProperties() ).isEmpty();

            Node p4 = member.getStartNode();
            assertFalse( relationships.hasNext() );
            assertThat( p4.getLabels() ).containsOnly( PERSON_LABEL );
            assertThat( p4.getAllProperties() ).containsOnly( entry( "id", 126 ) );
            assertThat( p4.getRelationships() ).hasSize( 1 );

            List<Node> division = nodes.stream().filter( node -> node.hasLabel( DIVISION_LABEL ) ).collect( Collectors.toList() );
            assertThat( division ).hasSize( 1 );
            Node d1 = division.get( 0 );
            assertThat( d1.getLabels() ).containsOnly( DIVISION_LABEL );
            assertThat( d1.getAllProperties() ).containsOnly( entry( "name", "Marketing" ) );
            assertThat( d1.getRelationships() ).isEmpty();

            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    /**
     * <pre>
     * (p1 :Person :S1 {id:123, name: “Ava”})
     * (p2 :Person :S2 {id:124, name: “Bob”})
     * (p3 :Person :S3 {id:125, name: “Cat”, age: 54})
     * (p4 :Person :S3 {id:126, name: “Dan”})
     *
     * (t1 :Team :SAll {name: “Foo”})
     * (t2 :Team :SAll {name: “Bar”})
     *
     * (d1 :Division :SAll {name: “Marketing”})
     *
     * (p1)-[:MEMBER]->(t1) (p2)-[:MEMBER]->(t2)
     * (p3)-[:MEMBER]->(t1) (p4)-[:MEMBER]->(t2)
     * </pre>
     */
    private void createOriginalGraph()
    {
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node p1 = tx.createNode( PERSON_LABEL, S1 );
            Node p2 = tx.createNode( PERSON_LABEL, S2 );
            Node p3 = tx.createNode( PERSON_LABEL, S3 );
            Node p4 = tx.createNode( PERSON_LABEL, S3 );
            p1.setProperty( "id", 123 );
            p1.setProperty( "name", "Ava" );
            p2.setProperty( "id", 124 );
            p2.setProperty( "name", "Bob" );
            p3.setProperty( "id", 125 );
            p3.setProperty( "name", "Cat" );
            p3.setProperty( "age", 54 );
            p4.setProperty( "id", 126 );
            p4.setProperty( "name", "Dan" );

            Node t1 = tx.createNode( TEAM_LABEL, SAll );
            Node t2 = tx.createNode( TEAM_LABEL, SAll );
            t1.setProperty( "name", "Foo" );
            t2.setProperty( "name", "Bar" );

            Node d1 = tx.createNode( DIVISION_LABEL, SAll );
            d1.setProperty( "name", "Marketing" );

            p1.createRelationshipTo( t1, MEMBER );
            p2.createRelationshipTo( t2, MEMBER );
            p3.createRelationshipTo( t1, MEMBER );
            p4.createRelationshipTo( t2, MEMBER );

            tx.commit();
        }
    }

    private void copyDatabase( String... args ) throws Exception
    {
        var context = new ExecutionContext( neo4jHome, configDir );
        var command = new StoreCopyCommand( context );

        CommandLine.populateCommand( command, args );
        command.execute();
    }
}
