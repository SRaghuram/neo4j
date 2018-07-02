/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.String.format;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FulltextProceduresTest
{
    static final String AWAIT_POPULATION = "CALL fulltext.awaitPopulation(\"%s\")";
    static final String GET_SCHEMA = "CALL fulltext.getIndexSchema(\"%s\")";
    static final String NODE_CREATE = "CALL fulltext.createNodeIndex(\"%s\", %s, %s )";
    static final String NODE_ANY_CREATE = "CALL fulltext.createAnyNodeLabelIndex(\"%s\", %s)";
    static final String RELATIONSHIP_CREATE = "CALL fulltext.createRelationshipIndex(\"%s\", %s, %s)";
    static final String RELATIONSHIP_ANY_CREATE = "CALL fulltext.createAnyRelationshipTypeIndex(\"%s\", %s)";
    static final String DROP = "CALL fulltext.dropIndex(\"%s\")";
    static final String STATUS = "CALL fulltext.indexStatus(\"%s\")";
    static final String QUERY = "CALL fulltext.query(\"%s\", \"%s\")";
    static final String ENTITYID = "entityId";
    static final String SCORE = "score";

    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private GraphDatabaseService db;
    private GraphDatabaseBuilder builder;

    @Before
    public void before()
    {
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
    }

    @After
    public void after()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    private GraphDatabaseService getDb() throws KernelException
    {
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, "false" );
        GraphDatabaseService db = builder.newGraphDatabase();
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( Procedures.class ).registerProcedure( FulltextProcedures.class );
        return db;
    }

    @Test
    public void createNodeFulltextIndex() throws Exception
    {
        db = getDb();
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
        db = getDb();
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
    public void createAnyNodeFulltextIndex() throws Exception
    {
        db = getDb();
        db.execute( format( NODE_ANY_CREATE, "test-index", array( "prop1", "prop2" ) ) ).close();
        Result result = db.execute( format( GET_SCHEMA, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "NODE:(prop1, prop2)", result.next().get( "schema" ) );
        assertFalse( result.hasNext() );
        db.execute( format( AWAIT_POPULATION, "test-index" ) ).close();
        result = db.execute( format( STATUS, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
        db.shutdown();
        db = getDb();
        result = db.execute( format( GET_SCHEMA, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "NODE:(prop1, prop2)", result.next().get( "schema" ) );
        assertFalse( result.hasNext() );
        result = db.execute( format( STATUS, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void createRelationshipFulltextIndex() throws Exception
    {
        db = getDb();
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
        db = getDb();
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
    public void createAnyRelationshipFulltextIndex() throws Exception
    {
        db = getDb();
        db.execute( format( RELATIONSHIP_ANY_CREATE, "test-index", array( "prop1", "prop2" ) ) ).close();
        Result result = db.execute( format( GET_SCHEMA, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "RELATIONSHIP:(prop1, prop2)", result.next().get( "schema" ) );
        assertFalse( result.hasNext() );
        db.execute( format( AWAIT_POPULATION, "test-index" ) ).close();
        result = db.execute( format( STATUS, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
        db.shutdown();
        db = getDb();
        result = db.execute( format( GET_SCHEMA, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "RELATIONSHIP:(prop1, prop2)", result.next().get( "schema" ) );
        assertFalse( result.hasNext() );
        result = db.execute( format( STATUS, "test-index" ) );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void dropIndex() throws Exception
    {
        db = getDb();
        db.execute( format( NODE_ANY_CREATE, "node-any", array( "prop1", "prop2" ) ) ).close();
        db.execute( format( NODE_CREATE, "node", array( "Label1", "Label2" ), array( "prop1", "prop2" ) ) ).close();
        db.execute( format( RELATIONSHIP_ANY_CREATE, "rel-any", array( "prop1", "prop2" ) ) ).close();
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

        db.execute( format( DROP, "rel-any" ) );
        indexes.remove( "rel-any" );
        newIndexes.clear();
        db.execute( "call db.indexes" ).forEachRemaining( m -> newIndexes.put( (String) m.get( "indexName" ), (String) m.get( "description" ) ) );
        assertEquals( indexes, newIndexes );

        db.execute( format( DROP, "node-any" ) );
        indexes.remove( "node-any" );
        newIndexes.clear();
        db.execute( "call db.indexes" ).forEachRemaining( m -> newIndexes.put( (String) m.get( "indexName" ), (String) m.get( "description" ) ) );
        assertEquals( indexes, newIndexes );
    }

    @Test
    public void shouldNotBeAbleToCreateTwoIndexesWithSameName() throws Exception
    {
        db = getDb();
        db.execute( format( NODE_CREATE, "node", array( "Label1", "Label2" ), array( "prop1", "prop2" ) ) ).close();
        expectedException.expectMessage( "already exists" );
        db.execute( format( NODE_CREATE, "node", array( "Label1", "Label2" ), array( "prop3", "prop4" ) ) ).close();
    }

    @Test
    public void indexConfigurationChangesAndTokenCreatesMustBePossibleInSameTransaction() throws Exception
    {
        db = getDb();

        try ( Transaction tx = db.beginTx() )
        {
            // The property keys we ask for do not exist, so those tokens will have to be allocated.
            // This test verifies that the locking required for the index modifications do not conflict with the
            // locking required for the token allocation.
            db.execute( format( NODE_ANY_CREATE, "nodesA", array( "this" ) ) );
            db.execute( format( RELATIONSHIP_ANY_CREATE, "relsA", array( "foo" ) ) );
            db.execute( format( NODE_ANY_CREATE, "nodesB", array( "that" ) ) );
            db.execute( format( RELATIONSHIP_ANY_CREATE, "relsB", array( "bar" ) ) );
        }
    }

    @Test
    public void creatingLabelsAfterAnyTokenIndexModificationMustNotBlockForever() throws Exception
    {
        db = getDb();

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_ANY_CREATE, "nodesA", array( "this" ) ) );
            db.execute( format( RELATIONSHIP_ANY_CREATE, "relsA", array( "foo" ) ) );
            Node node = db.getNodeById( nodeId );
            expectedException.expect( RuntimeException.class );
            node.addLabel( Label.label( "SOME_LABEL" ) );
            tx.success();
        }
    }

    @Test
    public void creatingRelTypesAfterAnyTokenIndexModificationMustNotBlockForever() throws Exception
    {
        db = getDb();

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_ANY_CREATE, "nodesA", array( "this" ) ) );
            db.execute( format( RELATIONSHIP_ANY_CREATE, "relsA", array( "foo" ) ) );
            Node node = db.getNodeById( nodeId );
            expectedException.expect( RuntimeException.class );
            node.createRelationshipTo( node, RelationshipType.withName( "SOME_REL" ) );
            tx.success();
        }
    }

    @Test
    public void anyLabelTokenIndexModificationAfterCreatingLabelsAndRelationshipTypesMustNotBlockForever() throws Exception
    {
        db = getDb();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.addLabel( Label.label( "SOME_LABEL" ) );
            node.createRelationshipTo( node, RelationshipType.withName( "SOME_REL" ) );
            expectedException.expect( RuntimeException.class );
            db.execute( format( NODE_ANY_CREATE, "nodesA", array( "this" ) ) );
        }
    }

    @Test
    public void anyRelTypeTokenIndexModificationAfterCreatingLabelsAndRelationshipTypesMustNotBlockForever() throws Exception
    {
        db = getDb();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.addLabel( Label.label( "SOME_LABEL" ) );
            node.createRelationshipTo( node, RelationshipType.withName( "SOME_REL" ) );
            expectedException.expect( RuntimeException.class );
            db.execute( format( RELATIONSHIP_ANY_CREATE, "relsA", array( "foo" ) ) );
        }
    }

    @Test
    public void creatingIndexesWhichImpliesTokenCreateMustNotBlockForever() throws Exception
    {
        db = getDb();

        // In fact, there should be no conflict here at all, so there is no exception to expect.
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodesA", array( "SOME_LABEL" ), array( "prop" ) ) );
            db.execute( format( RELATIONSHIP_CREATE, "relsA", array( "SOME_REL_TYPE" ), array( "prop" ) ) );
            db.execute( format( NODE_CREATE, "nodesB", array( "SOME_OTHER_LABEL" ), array( "prop" ) ) );
            db.execute( format( RELATIONSHIP_CREATE, "relsB", array( "SOME_OTHER_REL_TYPE" ), array( "prop" ) ) );
        }
    }

    @Test
    public void creatingAnyTokenIndexAndThenTokenIndexWhichImpliesLabelTokenCreateMustNotBlockForever() throws Exception
    {
        db = getDb();

        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_ANY_CREATE, "nodesA", array( "prop" ) ) );
            db.execute( format( RELATIONSHIP_ANY_CREATE, "relsA", array( "prop" ) ) );
            expectedException.expect( RuntimeException.class );
            db.execute( format( NODE_CREATE, "nodesB", array( "SOME_LABEL" ), array( "prop" ) ) );
        }
    }

    @Test
    public void creatingAnyTokenIndexAndThenTokenIndexWhichImpliesRelTypeTokenCreateMustNotBlockForever() throws Exception
    {
        db = getDb();

        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_ANY_CREATE, "nodesA", array( "prop" ) ) );
            db.execute( format( RELATIONSHIP_ANY_CREATE, "relsA", array( "prop" ) ) );
            expectedException.expect( RuntimeException.class );
            db.execute( format( RELATIONSHIP_CREATE, "relsB", array( "SOME_REL_TYPE" ), array( "prop" ) ) );
        }
    }

    @Test
    public void queryFulltext() throws Exception
    {
        db = getDb();
        db.execute( format( NODE_ANY_CREATE, "node-any", array( "prop1", "prop2" ) ) ).close();
        db.execute( format( NODE_CREATE, "node", array( "Label1", "Label2" ), array( "prop1", "prop2" ) ) ).close();
        db.execute( format( RELATIONSHIP_ANY_CREATE, "rel-any", array( "prop1", "prop2" ) ) ).close();
        db.execute( format( RELATIONSHIP_CREATE, "rel", array( "Reltype1", "Reltype2" ), array( "prop1", "prop2" ) ) ).close();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( AWAIT_POPULATION, "node-any" ) ).close();
            db.execute( format( AWAIT_POPULATION, "node" ) ).close();
            db.execute( format( AWAIT_POPULATION, "rel-any" ) ).close();
            db.execute( format( AWAIT_POPULATION, "node" ) ).close();
        }
        long zebraId;
        long horseId;
        long loopId;
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

            zebraId = zebra.getId();
            horseId = horse.getId();
            horseRelId = horseRel.getId();
            loopId = loop.getId();
            tx.success();
        }
        assertQueryFindsIds( db, "node-any", "zebra", zebraId );
        assertQueryFindsIds( db, "node-any", "zebra horse", zebraId, horseId );
        assertQueryFindsIds( db, "node", "horse", horseId );
        assertQueryFindsIds( db, "node", "horse zebra", horseId );

        assertQueryFindsIds( db, "rel-any", "zebra", loopId );
        assertQueryFindsIds( db, "rel-any", "zebra horse", horseRelId, loopId );
        assertQueryFindsIds( db, "rel", "horse", horseRelId );
        assertQueryFindsIds( db, "rel", "horse zebra", horseRelId );
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

    private String array( String... args )
    {
        return Arrays.stream( args ).map( s -> "\"" + s + "\"" ).collect( Collectors.joining( ", ", "[", "]" ) );
    }
}
