/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextAnalyzerTest;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.String.format;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FulltextProceduresTest
{
    static final String AWAIT_POPULATION = "CALL db.index.fulltext.awaitPopulation(\"%s\")";
    static final String GET_SCHEMA = "CALL db.index.fulltext.getIndexSchema(\"%s\")";
    static final String NODE_CREATE = "CALL db.index.fulltext.createNodeIndex(\"%s\", %s, %s )";
    static final String RELATIONSHIP_CREATE = "CALL db.index.fulltext.createRelationshipIndex(\"%s\", %s, %s)";
    static final String DROP = "CALL db.index.fulltext.dropIndex(\"%s\")";
    static final String STATUS = "CALL db.index.fulltext.indexStatus(\"%s\")";
    static final String QUERY = "CALL db.index.fulltext.query(\"%s\", \"%s\")";
    static final String ENTITYID = "entityId";
    static final String SCORE = "score";

    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    private final CleanupRule cleanup = new CleanupRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( fs ).around( testDirectory ).around( expectedException ).around( cleanup );

    private GraphDatabaseService db;
    private GraphDatabaseBuilder builder;

    @Before
    public void before()
    {
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, "false" );
    }

    private GraphDatabaseService createDatabase()
    {
        return cleanup.add( builder.newGraphDatabase() );
    }

    @Test
    public void createNodeFulltextIndex()
    {
        db = createDatabase();
        db.execute( format( NODE_CREATE, "test-index", array( "Label1", "Label2" ), array( "prop1", "prop2" ) ) ).close();
        Result result = db.execute( format( GET_SCHEMA, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "NODE:Label1, Label2(prop1, prop2)", result.next().get( "schema" ) );
        assertFalse( result.hasNext() );
        db.execute( format( AWAIT_POPULATION, "test-index" ) ).close();
        result = db.execute( format( STATUS, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
        db.shutdown();
        db = createDatabase();
        result = db.execute( format( GET_SCHEMA, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "NODE:Label1, Label2(prop1, prop2)", result.next().get( "schema" ) );
        assertFalse( result.hasNext() );
        result = db.execute( format( STATUS, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void createRelationshipFulltextIndex()
    {
        db = createDatabase();
        db.execute( format( RELATIONSHIP_CREATE, "test-index", array( "Reltype1", "Reltype2" ), array( "prop1", "prop2" ) ) ).close();
        Result result = db.execute( format( GET_SCHEMA, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "RELATIONSHIP:Reltype1, Reltype2(prop1, prop2)", result.next().get( "schema" ) );
        assertFalse( result.hasNext() );
        db.execute( format( AWAIT_POPULATION, "test-index" ) ).close();
        result = db.execute( format( STATUS, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
        db.shutdown();
        db = createDatabase();
        result = db.execute( format( GET_SCHEMA, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "RELATIONSHIP:Reltype1, Reltype2(prop1, prop2)", result.next().get( "schema" ) );
        assertFalse( result.hasNext() );
        result = db.execute( format( STATUS, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void dropIndex()
    {
        db = createDatabase();
        db.execute( format( NODE_CREATE, "node", array( "Label1", "Label2" ), array( "prop1", "prop2" ) ) ).close();
        db.execute( format( RELATIONSHIP_CREATE, "rel", array( "Reltype1", "Reltype2" ), array( "prop1", "prop2" ) ) ).close();
        Map<String,String> indexes = new HashMap<>();
        db.execute( "call db.indexes" ).forEachRemaining( m -> indexes.put( (String) m.get( "indexName" ), (String) m.get( "description" ) ) );

        db.execute( format( DROP, "node" ) );
        indexes.remove( "node" );
        Map<String,String> newIndexes = new HashMap<>();
        db.execute( "call db.indexes" ).forEachRemaining( m -> newIndexes.put( (String) m.get( "indexName" ), (String) m.get( "description" ) ) );
        assertEquals( indexes, newIndexes );

        db.execute( format( DROP, "rel" ) );
        indexes.remove( "rel" );
        newIndexes.clear();
        db.execute( "call db.indexes" ).forEachRemaining( m -> newIndexes.put( (String) m.get( "indexName" ), (String) m.get( "description" ) ) );
        assertEquals( indexes, newIndexes );
    }

    @Test
    public void mustNotBeAbleToCreateTwoIndexesWithSameName()
    {
        db = createDatabase();
        db.execute( format( NODE_CREATE, "node", array( "Label1", "Label2" ), array( "prop1", "prop2" ) ) ).close();
        expectedException.expectMessage( "already exists" );
        db.execute( format( NODE_CREATE, "node", array( "Label1", "Label2" ), array( "prop3", "prop4" ) ) ).close();
    }

    @Test
    public void nodeIndexesMustHaveLabels()
    {
        db = createDatabase();
        expectedException.expect( QueryExecutionException.class );
        db.execute( format( NODE_CREATE, "nodeIndex", array(), array( "prop" ) ) ).close();
    }

    @Test
    public void relationshipIndexesMustHaveRelationshipTypes()
    {
        db = createDatabase();
        expectedException.expect( QueryExecutionException.class );
        db.execute( format( RELATIONSHIP_CREATE, "relIndex", array(), array( "prop" ) ) );
    }

    @Test
    public void nodeIndexesMustHaveProperties()
    {
        db = createDatabase();
        expectedException.expect( QueryExecutionException.class );
        db.execute( format( NODE_CREATE, "nodeIndex", array( "Label" ), array() ) ).close();
    }

    @Test
    public void relationshipIndexesMustHaveProperties()
    {
        db = createDatabase();
        expectedException.expect( QueryExecutionException.class );
        db.execute( format( RELATIONSHIP_CREATE, "relIndex", array( "RELTYPE" ), array() ) );
    }

    @Test
    public void creatingIndexesWhichImpliesTokenCreateMustNotBlockForever()
    {
        db = createDatabase();

        try ( Transaction ignore = db.beginTx() )
        {
            // The property keys and labels we ask for do not exist, so those tokens will have to be allocated.
            // This test verifies that the locking required for the index modifications do not conflict with the
            // locking required for the token allocation.
            db.execute( format( NODE_CREATE, "nodesA", array( "SOME_LABEL" ), array( "this" ) ) );
            db.execute( format( RELATIONSHIP_CREATE, "relsA", array( "SOME_REL_TYPE" ), array( "foo" ) ) );
            db.execute( format( NODE_CREATE, "nodesB", array( "SOME_OTHER_LABEL" ), array( "that" ) ) );
            db.execute( format( RELATIONSHIP_CREATE, "relsB", array( "SOME_OTHER_REL_TYPE" ), array( "bar" ) ) );
        }
    }

    @Test
    public void creatingIndexWithSpecificAnalyzerMustUseThatAnalyzerForPopulationUpdatingAndQuerying()
    {
        db = createDatabase();
        LongHashSet noResults = new LongHashSet();
        LongHashSet swedishNodes = new LongHashSet();
        LongHashSet englishNodes = new LongHashSet();
        LongHashSet swedishRels = new LongHashSet();
        LongHashSet englishRels = new LongHashSet();

        Label label = Label.label( "LABEL" );
        RelationshipType relType = RelationshipType.withName( "REL" );
        String prop = "prop";
        String labelledSwedishNodes = "labelledSwedishNodes";
        String typedSwedishRelationships = "typedSwedishRelationships";

        try ( Transaction tx = db.beginTx() )
        {
            // Nodes and relationships picked up by index population.
            Node nodeA = db.createNode( label );
            nodeA.setProperty( prop, "En apa och en tomte bodde i ett hus." );
            swedishNodes.add( nodeA.getId() );
            Node nodeB = db.createNode( label );
            nodeB.setProperty( prop, "Hello and hello again, in the end." );
            englishNodes.add( nodeB.getId() );
            Relationship relA = nodeA.createRelationshipTo( nodeB, relType );
            relA.setProperty( prop, "En apa och en tomte bodde i ett hus." );
            swedishRels.add( relA.getId() );
            Relationship relB = nodeB.createRelationshipTo( nodeA, relType );
            relB.setProperty( prop, "Hello and hello again, in the end." );
            englishRels.add( relB.getId() );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            String lbl = array( label.name() );
            String rel = array( relType.name() );
            String props = array( prop );
            String swedish = props + ", {analyzer: '" + FulltextAnalyzerTest.SWEDISH + "'}";
            db.execute( format( NODE_CREATE, labelledSwedishNodes, lbl, swedish ) ).close();
            db.execute( format( RELATIONSHIP_CREATE, typedSwedishRelationships, rel, swedish ) ).close();
            tx.success();
        }
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( format( AWAIT_POPULATION, labelledSwedishNodes ) );
            db.execute( format( AWAIT_POPULATION, typedSwedishRelationships ) );
        }
        try ( Transaction tx = db.beginTx() )
        {
            // Nodes and relationships picked up by index updates.
            Node nodeC = db.createNode( label );
            nodeC.setProperty( prop, "En apa och en tomte bodde i ett hus." );
            swedishNodes.add( nodeC.getId() );
            Node nodeD = db.createNode( label );
            nodeD.setProperty( prop, "Hello and hello again, in the end." );
            englishNodes.add( nodeD.getId() );
            Relationship relC = nodeC.createRelationshipTo( nodeD, relType );
            relC.setProperty( prop, "En apa och en tomte bodde i ett hus." );
            swedishRels.add( relC.getId() );
            Relationship relD = nodeD.createRelationshipTo( nodeC, relType );
            relD.setProperty( prop, "Hello and hello again, in the end." );
            englishRels.add( relD.getId() );
            tx.success();
        }
        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( db, db::getNodeById, labelledSwedishNodes, "and", englishNodes ); // english word
            // swedish stop word (ignored by swedish analyzer, and not among the english nodes)
            assertQueryFindsIds( db, db::getNodeById, labelledSwedishNodes, "ett", noResults );
            assertQueryFindsIds( db, db::getNodeById, labelledSwedishNodes, "apa", swedishNodes ); // swedish word

            assertQueryFindsIds( db, db::getRelationshipById, typedSwedishRelationships, "and", englishRels );
            assertQueryFindsIds( db, db::getRelationshipById, typedSwedishRelationships, "ett", noResults );
            assertQueryFindsIds( db, db::getRelationshipById, typedSwedishRelationships, "apa", swedishRels );
        }
    }

    @Test
    public void queryShouldFindDataAddedInLaterTransactions()
    {
        db = createDatabase();
        db.execute( format( NODE_CREATE, "node", array( "Label1", "Label2" ), array( "prop1", "prop2" ) ) ).close();
        db.execute( format( RELATIONSHIP_CREATE, "rel", array( "Reltype1", "Reltype2" ), array( "prop1", "prop2" ) ) ).close();
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( format( AWAIT_POPULATION, "node" ) ).close();
            db.execute( format( AWAIT_POPULATION, "node" ) ).close();
        }
        long horseId;
        long horseRelId;
        try ( Transaction tx = db.beginTx() )
        {
            Node zebra = db.createNode();
            zebra.setProperty( "prop1", "zebra" );
            Node horse = db.createNode( Label.label( "Label1" ) );
            horse.setProperty( "prop2", "horse" );
            horse.setProperty( "prop3", "zebra" );
            Relationship horseRel = zebra.createRelationshipTo( horse, RelationshipType.withName( "Reltype1" ) );
            horseRel.setProperty( "prop1", "horse" );
            Relationship loop = horse.createRelationshipTo( horse, RelationshipType.withName( "loop" ) );
            loop.setProperty( "prop2", "zebra" );

            horseId = horse.getId();
            horseRelId = horseRel.getId();
            tx.success();
        }
        assertQueryFindsIds( db, "node", "horse", horseId );
        assertQueryFindsIds( db, "node", "horse zebra", horseId );

        assertQueryFindsIds( db, "rel", "horse", horseRelId );
        assertQueryFindsIds( db, "rel", "horse zebra", horseRelId );
    }

    @Test
    public void queryShouldFindDataAddedInIndexPopulation()
    {
        // when
        Label label = Label.label( "LABEL" );
        RelationshipType relType = RelationshipType.withName( "REL" );
        Node node1;
        Node node2;
        Relationship relationship;
        db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.createNode( label );
            node1.setProperty( "prop", "This is a integration test." );
            node2 = db.createNode( label );
            node2.setProperty( "otherprop", "This is a related integration test" );
            relationship = node1.createRelationshipTo( node2, relType );
            relationship.setProperty( "prop", "They relate" );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "node", array( label.name() ), array( "prop", "otherprop" ) ) );
            db.execute( format( RELATIONSHIP_CREATE, "rel", array( relType.name() ), array( "prop" ) ) );
            tx.success();
        }
        db.execute( format( AWAIT_POPULATION, "node" ) ).close();
        db.execute( format( AWAIT_POPULATION, "rel" ) ).close();

        // then
        assertQueryFindsIds( db, "node", "integration", node1.getId(), node2.getId() );
        assertQueryFindsIds( db, "node", "test", node1.getId(), node2.getId() );
        assertQueryFindsIds( db, "node", "related", node2.getId() );
        assertQueryFindsIds( db, "rel", "relate", relationship.getId() );
    }

    private void assertQueryFindsIds( GraphDatabaseService db, String index, String query, long... ids )
    {
        Result result = db.execute( format( QUERY, index, query ) );
        int num = 0;
        Double score = Double.MAX_VALUE;
        while ( result.hasNext() )
        {
            Map entry = result.next();
            Long nextId = (Long) entry.get( ENTITYID );
            Double nextScore = (Double) entry.get( SCORE );
            assertThat( nextScore, lessThanOrEqualTo( score ) );
            score = nextScore;
            assertEquals( String.format( "Result returned id %d, expected %d", nextId, ids[num] ), ids[num], nextId.longValue() );
            num++;
        }
        assertEquals( "Number of results differ from expected", ids.length, num );
    }

    private void assertQueryFindsIds(
            GraphDatabaseService db, LongFunction<Entity> getEntity, String index, String query, LongHashSet ids )
    {
        ids = new LongHashSet( ids ); // Create a defensive copy, because we're going to modify this instance.
        long[] expectedIds = ids.toArray();
        MutableLongSet actualIds = new LongHashSet();
        Result result = db.execute( format( QUERY, index, query ) );
        Double score = Double.MAX_VALUE;
        while ( result.hasNext() )
        {
            Map entry = result.next();
            Long nextId = (Long) entry.get( ENTITYID );
            Double nextScore = (Double) entry.get( SCORE );
            assertThat( nextScore, lessThanOrEqualTo( score ) );
            score = nextScore;
            actualIds.add( nextId );
            if ( !ids.remove( nextId ) )
            {
                String msg = "This id was not expected: " + nextId;
                failQuery( getEntity, index, query, ids, expectedIds, actualIds, msg );
            }
        }
        if ( !ids.isEmpty() )
        {
            String msg = "Not all expected ids were found: " + ids;
            failQuery( getEntity, index, query, ids, expectedIds, actualIds, msg );
        }
    }

    private void failQuery( LongFunction<Entity> getEntity, String index, String query, MutableLongSet ids,
                            long[] expectedIds, MutableLongSet actualIds, String msg )
    {
        StringBuilder message = new StringBuilder( msg ).append( '\n' );
        MutableLongIterator itr = ids.longIterator();
        while ( itr.hasNext() )
        {
            long id = itr.next();
            Entity entity = getEntity.apply( id );
            message.append( '\t' ).append( entity ).append( entity.getAllProperties() ).append( '\n' );
        }
        message.append( "for query: '" ).append( query ).append( "'\nin index: " ).append( index ).append( '\n' );
        message.append( "all expected ids: " ).append( Arrays.toString( expectedIds ) ).append( '\n' );
        message.append( "actual ids: " ).append( actualIds );
        itr = actualIds.longIterator();
        while ( itr.hasNext() )
        {
            long id = itr.next();
            Entity entity = getEntity.apply( id );
            message.append( "\n\t" ).append( entity ).append( entity.getAllProperties() );
        }
        fail( message.toString() );
    }

    static String array( String... args )
    {
        return Arrays.stream( args ).map( s -> "\"" + s + "\"" ).collect( Collectors.joining( ", ", "[", "]" ) );
    }
}
