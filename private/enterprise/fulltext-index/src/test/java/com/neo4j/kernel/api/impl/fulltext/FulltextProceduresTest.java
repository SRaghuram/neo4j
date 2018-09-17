/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextAnalyzerTest;
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.ThreadTestUtils;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.VerboseTimeout;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.ONLY;

public class FulltextProceduresTest
{
    private static final String DB_INDEXES = "CALL db.indexes";
    private static final String DROP = "CALL db.index.fulltext.drop(\"%s\")";
    private static final String LIST_AVAILABLE_ANALYZERS = "CALL db.index.fulltext.listAvailableAnalyzers()";
    private static final String QUERY_NODES = "CALL db.index.fulltext.queryNodes(\"%s\", \"%s\")";
    private static final String QUERY_RELS = "CALL db.index.fulltext.queryRelationships(\"%s\", \"%s\")";
    static final String AWAIT_REFRESH = "CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()";
    static final String NODE_CREATE = "CALL db.index.fulltext.createNodeIndex(\"%s\", %s, %s )";
    static final String RELATIONSHIP_CREATE = "CALL db.index.fulltext.createRelationshipIndex(\"%s\", %s, %s)";
    static final String QUERY = "CALL db.index.fulltext.query(\"%s\", \"%s\")";

    private static final String SCORE = "score";
    static final String ENTITYID = "entityId";
    public static final String DESCARTES_MEDITATIONES = "/meditationes--rene-descartes--public-domain.txt";

    private final Timeout timeout = VerboseTimeout.builder().withTimeout( 1, TimeUnit.MINUTES ).build();
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    private final CleanupRule cleanup = new CleanupRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( timeout ).around( fs ).around( testDirectory ).around( expectedException ).around( cleanup );

    private GraphDatabaseAPI db;
    private GraphDatabaseBuilder builder;

    @Before
    public void before()
    {
        GraphDatabaseFactory factory = new EnterpriseGraphDatabaseFactory();
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.databaseDir() );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, "false" );
    }

    @After
    public void tearDown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void createNodeFulltextIndex()
    {
        db = createDatabase();
        db.execute( format( NODE_CREATE, "test-index", array( "Label1", "Label2" ), array( "prop1", "prop2" ) ) ).close();
        Result result = db.execute( DB_INDEXES );
        assertTrue( result.hasNext() );
        Map<String,Object> row = result.next();
        assertEquals( "INDEX ON NODE:Label1, Label2(prop1, prop2)", row.get( "description" ) );
        assertEquals( asList( "Label1", "Label2" ), row.get( "tokenNames" ) );
        assertEquals( asList( "prop1", "prop2" ), row.get( "properties" ) );
        assertEquals( "test-index", row.get( "indexName" ) );
        assertFalse( result.hasNext() );
        result.close();
        awaitIndexesOnline();
        result = db.execute( DB_INDEXES );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
        result.close();
        db.shutdown();
        db = createDatabase();
        result = db.execute( DB_INDEXES );
        assertTrue( result.hasNext() );
        row = result.next();
        assertEquals( "INDEX ON NODE:Label1, Label2(prop1, prop2)", row.get( "description" ) );
        assertEquals( "ONLINE", row.get( "state" ) );
        assertFalse( result.hasNext() );
        assertFalse( result.hasNext() );
    }

    @Test
    public void createRelationshipFulltextIndex()
    {
        db = createDatabase();
        db.execute( format( RELATIONSHIP_CREATE, "test-index", array( "Reltype1", "Reltype2" ), array( "prop1", "prop2" ) ) ).close();
        Result result = db.execute( DB_INDEXES );
        assertTrue( result.hasNext() );
        Map<String,Object> row = result.next();
        assertEquals( "INDEX ON RELATIONSHIP:Reltype1, Reltype2(prop1, prop2)", row.get( "description" ) );
        assertEquals( asList( "Reltype1", "Reltype2" ), row.get( "tokenNames" ) );
        assertEquals( asList( "prop1", "prop2" ), row.get( "properties" ) );
        assertEquals( "test-index", row.get( "indexName" ) );
        assertFalse( result.hasNext() );
        result.close();
        awaitIndexesOnline();
        result = db.execute( DB_INDEXES );
        assertTrue( result.hasNext() );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
        result.close();
        db.shutdown();
        db = createDatabase();
        result = db.execute( DB_INDEXES );
        assertTrue( result.hasNext() );
        row = result.next();
        assertEquals( "INDEX ON RELATIONSHIP:Reltype1, Reltype2(prop1, prop2)", row.get( "description" ) );
        assertEquals( "ONLINE", row.get( "state" ) );
        assertFalse( result.hasNext() );
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
        awaitIndexesOnline();
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
        awaitIndexesOnline();
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
        awaitIndexesOnline();

        // then
        assertQueryFindsIds( db, "node", "integration", node1.getId(), node2.getId() );
        assertQueryFindsIds( db, "node", "test", node1.getId(), node2.getId() );
        assertQueryFindsIds( db, "node", "related", node2.getId() );
        assertQueryFindsIds( db, "rel", "relate", relationship.getId() );
    }

    @Test
    public void updatesToEventuallyConsistentIndexMustEventuallyBecomeVisible()
    {
        Label label = Label.label( "LABEL" );
        RelationshipType relType = RelationshipType.withName( "REL" );
        String prop = "prop";
        String value = "bla bla";
        String eventuallyConsistent = ", {eventually_consistent: 'true'}";
        db = createDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "node", array( label.name() ), array( prop ) + eventuallyConsistent ) );
            db.execute( format( RELATIONSHIP_CREATE, "rel", array( relType.name() ), array( prop ) + eventuallyConsistent ) );
            tx.success();
        }

        int entityCount = 200;
        LongHashSet nodeIds = new LongHashSet();
        LongHashSet relIds = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < entityCount; i++ )
            {
                Node node = db.createNode( label );
                node.setProperty( prop, value );
                Relationship rel = node.createRelationshipTo( node, relType );
                rel.setProperty( prop, value );
                nodeIds.add( node.getId() );
                relIds.add( rel.getId() );
            }
            tx.success();
        }

        // Assert that we can observe our updates within 20 seconds from now. We have, after all, already committed the transaction.
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 20 );
        boolean success = false;
        do
        {
            try
            {
                assertQueryFindsIds( db, db::getNodeById, "node", "bla", nodeIds );
                assertQueryFindsIds( db, db::getRelationshipById, "rel", "bla", relIds );
                success = true;
            }
            catch ( Throwable throwable )
            {
                if ( deadline <= System.currentTimeMillis() )
                {
                    // We're past the deadline. This test is not successful.
                    throw throwable;
                }
            }
        }
        while ( !success );
    }

    @Test
    public void updatesToEventuallyConsistentIndexMustBecomeVisibleAfterAwaitRefresh()
    {
        Label label = Label.label( "LABEL" );
        RelationshipType relType = RelationshipType.withName( "REL" );
        String prop = "prop";
        String value = "bla bla";
        String eventuallyConsistent = ", {eventually_consistent: 'true'}";
        db = createDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "node", array( label.name() ), array( prop ) + eventuallyConsistent ) );
            db.execute( format( RELATIONSHIP_CREATE, "rel", array( relType.name() ), array( prop ) + eventuallyConsistent ) );
            tx.success();
        }
        awaitIndexesOnline();

        int entityCount = 200;
        LongHashSet nodeIds = new LongHashSet();
        LongHashSet relIds = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < entityCount; i++ )
            {
                Node node = db.createNode( label );
                node.setProperty( prop, value );
                Relationship rel = node.createRelationshipTo( node, relType );
                rel.setProperty( prop, value );
                nodeIds.add( node.getId() );
                relIds.add( rel.getId() );
            }
            tx.success();
        }

        db.execute( AWAIT_REFRESH ).close();
        assertQueryFindsIds( db, db::getNodeById, "node", "bla", nodeIds );
        assertQueryFindsIds( db, db::getRelationshipById, "rel", "bla", relIds );
    }

    @Test
    public void eventuallyConsistentIndexMustPopulateWithExistingDataWhenCreated()
    {
        Label label = Label.label( "LABEL" );
        RelationshipType relType = RelationshipType.withName( "REL" );
        String prop = "prop";
        String value = "bla bla";
        String eventuallyConsistent = ", {eventually_consistent: 'true'}";
        db = createDatabase();

        int entityCount = 200;
        LongHashSet nodeIds = new LongHashSet();
        LongHashSet relIds = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < entityCount; i++ )
            {
                Node node = db.createNode( label );
                node.setProperty( prop, value );
                Relationship rel = node.createRelationshipTo( node, relType );
                rel.setProperty( prop, value );
                nodeIds.add( node.getId() );
                relIds.add( rel.getId() );
            }
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "node", array( label.name() ), array( prop ) + eventuallyConsistent ) );
            db.execute( format( RELATIONSHIP_CREATE, "rel", array( relType.name() ), array( prop ) + eventuallyConsistent ) );
            tx.success();
        }

        awaitIndexesOnline();
        assertQueryFindsIds( db, db::getNodeById, "node", "bla", nodeIds );
        assertQueryFindsIds( db, db::getRelationshipById, "rel", "bla", relIds );
    }

    @Test
    public void concurrentPopulationAndUpdatesToAnEventuallyConsistentIndexMustEventuallyResultInCorrectIndexState() throws Exception
    {
        Label label = Label.label( "LABEL" );
        RelationshipType relType = RelationshipType.withName( "REL" );
        String prop = "prop";
        String oldValue = "red";
        String newValue = "green";
        String eventuallyConsistent = ", {eventually_consistent: 'true'}";
        db = createDatabase();

        int entityCount = 200;
        LongHashSet nodeIds = new LongHashSet();
        LongHashSet relIds = new LongHashSet();

        // First we create the nodes and relationships with the property value "red".
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < entityCount; i++ )
            {
                Node node = db.createNode( label );
                node.setProperty( prop, oldValue );
                Relationship rel = node.createRelationshipTo( node, relType );
                rel.setProperty( prop, oldValue );
                nodeIds.add( node.getId() );
                relIds.add( rel.getId() );
            }
            tx.success();
        }

        // Then, in two concurrent transactions, we create our indexes AND change all the property values to "green".
        CountDownLatch readyLatch = new CountDownLatch( 2 );
        BinaryLatch startLatch = new BinaryLatch();
        Runnable createIndexes = () ->
        {
            readyLatch.countDown();
            startLatch.await();
            try ( Transaction tx = db.beginTx() )
            {
                db.execute( format( NODE_CREATE, "node", array( label.name() ), array( prop ) + eventuallyConsistent ) );
                db.execute( format( RELATIONSHIP_CREATE, "rel", array( relType.name() ), array( prop ) + eventuallyConsistent ) );
                tx.success();
            }
        };
        Runnable makeAllEntitiesGreen = () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                // Prepare our transaction state first.
                nodeIds.forEach( nodeId -> db.getNodeById( nodeId ).setProperty( prop, newValue ) );
                relIds.forEach( relId -> db.getRelationshipById( relId ).setProperty( prop, newValue ) );
                tx.success();
                // Okay, NOW we're ready to race!
                readyLatch.countDown();
                startLatch.await();
            }
        };
        ExecutorService executor = cleanup.add( Executors.newFixedThreadPool( 2 ) );
        Future<?> future1 = executor.submit( createIndexes );
        Future<?> future2 = executor.submit( makeAllEntitiesGreen );
        readyLatch.await();
        startLatch.release();

        // Finally, when everything has settled down, we should see that all of the nodes and relationships are indexed with the value "green".
        future1.get();
        future2.get();
        awaitIndexesOnline();
        db.execute( AWAIT_REFRESH ).close();
        assertQueryFindsIds( db, db::getNodeById, "node", newValue, nodeIds );
        assertQueryFindsIds( db, db::getRelationshipById, "rel", newValue, relIds );
    }

    @Test
    public void fulltextIndexesMustBeEventuallyConsistentByDefaultWhenThisIsConfigured() throws InterruptedException
    {
        builder.setConfig( FulltextConfig.eventually_consistent, Settings.TRUE );
        db = createDatabase();

        Label label = Label.label( "LABEL" );
        RelationshipType relType = RelationshipType.withName( "REL" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "node", array( label.name() ), array( "prop", "otherprop" ) ) );
            db.execute( format( RELATIONSHIP_CREATE, "rel", array( relType.name() ), array( "prop" ) ) );
            tx.success();
        }
        awaitIndexesOnline();

        // Prevent index updates from being applied to eventually consistent indexes.
        BinaryLatch indexUpdateBlocker = new BinaryLatch();
        db.getDependencyResolver().resolveDependency( JobScheduler.class, ONLY ).schedule( Group.INDEX_UPDATING, indexUpdateBlocker::await );

        LongHashSet nodeIds = new LongHashSet();
        long relId;
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = db.createNode( label );
                node1.setProperty( "prop", "bla bla" );
                Node node2 = db.createNode( label );
                node2.setProperty( "otherprop", "bla bla" );
                Relationship relationship = node1.createRelationshipTo( node2, relType );
                relationship.setProperty( "prop", "bla bla" );
                nodeIds.add( node1.getId() );
                nodeIds.add( node2.getId() );
                relId = relationship.getId();
                tx.success();
            }

            // Index updates are still blocked for eventually consistent indexes, so we should not find anything at this point.
            assertQueryFindsIds( db, db::getNodeById, "node", "bla", new LongHashSet() );
            assertQueryFindsIds( db, db::getRelationshipById, "rel", "bla", new LongHashSet() );
        }
        finally
        {
            // Uncork the eventually consistent fulltext index updates.
            Thread.sleep( 10 );
            indexUpdateBlocker.release();
        }
        // And wait for them to apply.
        db.execute( AWAIT_REFRESH );

        // Now we should see our data.
        assertQueryFindsIds( db, db::getNodeById, "node", "bla", nodeIds );
        assertQueryFindsIds( db, "rel", "bla", relId );
    }

    @Test
    public void mustBeAbleToListAvailableAnalyzers()
    {
        db = createDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            Set<String> analyzers = new HashSet<>();
            try ( ResourceIterator<String> iterator = db.execute( LIST_AVAILABLE_ANALYZERS ).columnAs( "analyzer" ) )
            {
                while ( iterator.hasNext() )
                {
                    analyzers.add( iterator.next() );
                }
            }
            assertThat( analyzers, hasItem( "english" ) );
            assertThat( analyzers, hasItem( "swedish" ) );
            assertThat( analyzers, hasItem( "standard" ) );
            tx.success();
        }
    }

    @Test
    public void queryNodesMustThrowWhenQueryingRelationshipIndex()
    {
        db = createDatabase();

        RelationshipType relType = RelationshipType.withName( "REL" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( relType.name() ), array( "prop" ) ) ).close();
            tx.success();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            expectedException.expect( Exception.class );
            db.execute( format( QUERY_NODES, "rels", "bla bla" ) ).close();
            tx.success();
        }
    }

    @Test
    public void queryRelationshipsMustThrowWhenQueryingNodeIndex()
    {
        db = createDatabase();

        Label label = Label.label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop" ) ) ).close();
            tx.success();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            expectedException.expect( Exception.class );
            db.execute( format( QUERY_RELS, "nodes", "bla bla" ) ).close();
            tx.success();
        }
    }

    @Test
    public void queryNodesMustFindSameNodeIdsAndScoreAsQuery()
    {
        db = createDatabase();

        Label label = Label.label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop" ) ) ).close();
            tx.success();
        }
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label ).setProperty( "prop", "yadda bla bla yadda" );
            db.createNode( label ).setProperty( "prop", "blim blim bla yadda" );
            db.createNode( label ).setProperty( "prop", "blue green bla grim fandango" );
            db.createNode( label ).setProperty( "prop", "bubbles balloon bla bla bla bubblegum" );
            db.createNode( label ).setProperty( "prop", "bada bing bada bla bada boom" );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Result expected = db.execute( format( QUERY, "nodes", "bla" ) );
            Result actual = db.execute( format( QUERY_NODES, "nodes", "bla " ) );
            while ( expected.hasNext() )
            {
                assertTrue( actual.hasNext() );
                Map<String,Object> expectedRow = expected.next();
                Map<String,Object> actualRow = actual.next();
                assertThat( actualRow.get( SCORE ), is( expectedRow.get( SCORE ) ) );
                assertThat( ((Node) actualRow.get( "node" )).getId(), is( expectedRow.get( ENTITYID ) ) );
            }
            assertFalse( actual.hasNext() );
            tx.success();
        }
    }

    @Test
    public void queryRelationshipsMustFindSameNodeIdsAndScoreAsQuery()
    {
        db = createDatabase();

        RelationshipType relType = RelationshipType.withName( "REL" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( relType.name() ), array( "prop" ) ) ).close();
            tx.success();
        }
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.createRelationshipTo( node, relType ).setProperty( "prop", "yadda bla bla yadda" );
            node.createRelationshipTo( node, relType ).setProperty( "prop", "blim blim bla yadda" );
            node.createRelationshipTo( node, relType ).setProperty( "prop", "blue green bla grim fandango" );
            node.createRelationshipTo( node, relType ).setProperty( "prop", "bubbles balloon bla bla bla bubblegum" );
            node.createRelationshipTo( node, relType ).setProperty( "prop", "bada bing bada bla bada boom" );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Result expected = db.execute( format( QUERY, "rels", "bla" ) );
            Result actual = db.execute( format( QUERY_RELS, "rels", "bla " ) );
            while ( expected.hasNext() )
            {
                assertTrue( actual.hasNext() );
                Map<String,Object> expectedRow = expected.next();
                Map<String,Object> actualRow = actual.next();
                assertThat( actualRow.get( SCORE ), is( expectedRow.get( SCORE ) ) );
                assertThat( ((Relationship) actualRow.get( "relationship" )).getId(), is( expectedRow.get( ENTITYID ) ) );
            }
            assertFalse( actual.hasNext() );
            tx.success();
        }
    }

    @Test
    public void fulltextIndexMustIgnoreNonStringPropertiesForUpdate()
    {
        db = createDatabase();

        Label label = Label.label( "Label" );
        RelationshipType relType = RelationshipType.withName( "REL" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop" ) ) ).close();
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( relType.name() ), array( "prop" ) ) ).close();
            tx.success();
        }

        awaitIndexesOnline();

        List<Value> values = generateRandomNonStringValues( 1000 );

        try ( Transaction tx = db.beginTx() )
        {
            for ( Value value : values )
            {
                Node node = db.createNode( label );
                Object propertyValue = value.asObject();
                node.setProperty( "prop", propertyValue );
                node.createRelationshipTo( node, relType ).setProperty( "prop", propertyValue );
            }
            tx.success();
        }

        for ( Value value : values )
        {
            String fulltextQuery = quoteValueForQuery( value );
            String cypherQuery = format( QUERY, "nodes", fulltextQuery );
            Result nodes;
            try
            {
                nodes = db.execute( cypherQuery );
            }
            catch ( QueryExecutionException e )
            {
                throw new AssertionError( "Failed to execute query: " + cypherQuery + " based on value " + value.prettyPrint(), e );
            }
            if ( nodes.hasNext() )
            {
                fail( "did not expect to find any nodes, but found at least: " + nodes.next() );
            }
            nodes.close();
            Result relationships = db.execute( format( QUERY, "rels", fulltextQuery ) );
            if ( relationships.hasNext() )
            {
                fail( "did not expect to find any relationships, but found at least: " + relationships.next() );
            }
            relationships.close();
        }
    }

    @Test
    public void fulltextIndexMustIgnoreNonStringPropertiesForPopulation()
    {
        db = createDatabase();

        Label label = Label.label( "Label" );
        RelationshipType relType = RelationshipType.withName( "REL" );

        List<Value> values = generateRandomNonStringValues( 1000 );

        try ( Transaction tx = db.beginTx() )
        {
            for ( Value value : values )
            {
                Node node = db.createNode( label );
                Object propertyValue = value.asObject();
                node.setProperty( "prop", propertyValue );
                node.createRelationshipTo( node, relType ).setProperty( "prop", propertyValue );
            }
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop" ) ) ).close();
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( relType.name() ), array( "prop" ) ) ).close();
            tx.success();
        }

        awaitIndexesOnline();

        for ( Value value : values )
        {
            String fulltextQuery = quoteValueForQuery( value );
            String cypherQuery = format( QUERY, "nodes", fulltextQuery );
            Result nodes;
            try
            {
                nodes = db.execute( cypherQuery );
            }
            catch ( QueryExecutionException e )
            {
                throw new AssertionError( "Failed to execute query: " + cypherQuery + " based on value " + value.prettyPrint(), e );
            }
            if ( nodes.hasNext() )
            {
                fail( "did not expect to find any nodes, but found at least: " + nodes.next() );
            }
            nodes.close();
            Result relationships = db.execute( format( QUERY, "rels", fulltextQuery ) );
            if ( relationships.hasNext() )
            {
                fail( "did not expect to find any relationships, but found at least: " + relationships.next() );
            }
            relationships.close();
        }
    }

    @Test
    public void entitiesMustBeRemovedFromFulltextIndexWhenPropertyValuesChangeAwayFromText()
    {
        db = createDatabase();

        Label label = Label.label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop" ) ) ).close();
            tx.success();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            nodeId = node.getId();
            node.setProperty( "prop", "bla bla" );
            tx.success();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( "prop", 42 );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Result result = db.execute( format( QUERY_NODES, "nodes", "bla" ) );
            assertFalse( result.hasNext() );
            result.close();
            tx.success();
        }
    }

    @Test
    public void entitiesMustBeAddedToFulltextIndexWhenPropertyValuesChangeToText()
    {
        db = createDatabase();

        Label label = Label.label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop" ) ) ).close();
            tx.success();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( "prop", 42 );
            nodeId = node.getId();
            tx.success();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( "prop", "bla bla" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, "nodes", "bla", nodeId );
            tx.success();
        }
    }

    @Test
    public void propertiesMustBeRemovedFromFulltextIndexWhenTheirValueTypeChangesAwayFromText()
    {
        db = createDatabase();

        Label label = Label.label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop1", "prop2" ) ) ).close();
            tx.success();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            nodeId = node.getId();
            node.setProperty( "prop1", "foo" );
            node.setProperty( "prop2", "bar" );
            tx.success();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( "prop2", 42 );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, "nodes", "foo", nodeId );
            Result result = db.execute( format( QUERY_NODES, "nodes", "bar" ) );
            assertFalse( result.hasNext() );
            result.close();
            tx.success();
        }
    }

    @Test
    public void propertiesMustBeAddedToFulltextIndexWhenTheirValueTypeChangesToText()
    {
        db = createDatabase();

        Label label = Label.label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop1", "prop2" ) ) ).close();
            tx.success();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            nodeId = node.getId();
            node.setProperty( "prop1", "foo" );
            node.setProperty( "prop2", 42 );
            tx.success();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( "prop2", "bar" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, "nodes", "foo", nodeId );
            assertQueryFindsIds( db, "nodes", "bar", nodeId );
            tx.success();
        }
    }

    @Test
    public void mustBeAbleToIndexHugeTextPropertiesInIndexUpdates() throws Exception
    {
        String meditationes;
        try ( BufferedReader reader = new BufferedReader(
                new InputStreamReader( getClass().getResourceAsStream( DESCARTES_MEDITATIONES ), StandardCharsets.UTF_8 ) ) )
        {
            meditationes = reader.lines().collect( Collectors.joining( "\n" ) );
        }

        db = createDatabase();

        Label label = Label.label( "Book" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "books", array( label.name() ), array( "title", "author", "contents" ) ) ).close();
            tx.success();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            nodeId = node.getId();
            node.setProperty( "title", "Meditationes de prima philosophia" );
            node.setProperty( "author", "René Descartes" );
            node.setProperty( "contents", meditationes );
            tx.success();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, "books", "impellit scriptum offerendum", nodeId );
            tx.success();
        }
    }

    @Test
    public void mustBeAbleToIndexHugeTextPropertiesInIndexPopulation() throws Exception
    {
        String meditationes;
        try ( BufferedReader reader = new BufferedReader(
                new InputStreamReader( getClass().getResourceAsStream( DESCARTES_MEDITATIONES ), StandardCharsets.UTF_8 ) ) )
        {
            meditationes = reader.lines().collect( Collectors.joining( "\n" ) );
        }

        db = createDatabase();

        Label label = Label.label( "Book" );
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            nodeId = node.getId();
            node.setProperty( "title", "Meditationes de prima philosophia" );
            node.setProperty( "author", "René Descartes" );
            node.setProperty( "contents", meditationes );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "books", array( label.name() ), array( "title", "author", "contents" ) ) ).close();
            tx.success();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, "books", "impellit scriptum offerendum", nodeId );
            tx.success();
        }
    }

    @Test
    public void mustBeAbleToQuerySpecificPropertiesViaLuceneSyntax()
    {
        db = createDatabase();
        Label book = Label.label( "Book" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "books", array( book.name() ), array( "title", "author" ) ) ).close();
            tx.success();
        }

        long book2id;
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node book1 = db.createNode( book );
            book1.setProperty( "author", "René Descartes" );
            book1.setProperty( "title", "Meditationes de prima philosophia" );
            Node book2 = db.createNode( book );
            book2.setProperty( "author", "E. M. Curley" );
            book2.setProperty( "title", "Descartes Against the Skeptics" );
            book2id = book2.getId();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            LongHashSet ids = LongHashSet.newSetWith( book2id );
            assertQueryFindsIds( db, db::getNodeById, "books", "title:Descartes", ids );
            tx.success();
        }
    }

    @Test
    public void queryResultsMustNotIncludeNodesDeletedInOtherConcurrentlyCommittedTransactions() throws Exception
    {
        db = createDatabase();
        Label label = Label.label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop" ) ) ).close();
            tx.success();
        }
        long nodeIdA;
        long nodeIdB;
        try ( Transaction tx = db.beginTx() )
        {
            awaitIndexesOnline();
            Node nodeA = db.createNode( label );
            nodeA.setProperty( "prop", "value" );
            nodeIdA = nodeA.getId();
            Node nodeB = db.createNode( label );
            nodeB.setProperty( "prop", "value" );
            nodeIdB = nodeB.getId();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = db.execute( format( QUERY_NODES, "nodes", "value" ) ) )
            {
                ThreadTestUtils.forkFuture( () ->
                {
                    try ( Transaction forkedTx = db.beginTx() )
                    {
                        db.getNodeById( nodeIdA ).delete();
                        db.getNodeById( nodeIdB ).delete();
                        forkedTx.success();
                    }
                    return null;
                } ).get();
                assertThat( result.stream().count(), is( 0L ) );
            }
            tx.success();
        }
    }

    @Test
    public void queryResultsMustNotIncludeRelationshipsDeletedInOtherConcurrentlyCommittedTransactions() throws Exception
    {
        db = createDatabase();
        RelationshipType relType = RelationshipType.withName( "REL" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( relType.name() ), array( "prop" ) ) ).close();
            tx.success();
        }
        long relIdA;
        long relIdB;
        try ( Transaction tx = db.beginTx() )
        {
            awaitIndexesOnline();
            Node node = db.createNode();
            Relationship relA = node.createRelationshipTo( node, relType );
            relA.setProperty( "prop", "value" );
            relIdA = relA.getId();
            Relationship relB = node.createRelationshipTo( node, relType );
            relB.setProperty( "prop", "value" );
            relIdB = relB.getId();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = db.execute( format( QUERY_RELS, "rels", "value" ) ) )
            {
                ThreadTestUtils.forkFuture( () ->
                {
                    try ( Transaction forkedTx = db.beginTx() )
                    {
                        db.getRelationshipById( relIdA ).delete();
                        db.getRelationshipById( relIdB ).delete();
                        forkedTx.success();
                    }
                    return null;
                } ).get();
                assertThat( result.stream().count(), is( 0L ) );
            }
            tx.success();
        }
    }

    @Test
    public void queryResultsMustNotIncludeNodesDeletedInThisTransaction()
    {
        db = createDatabase();
        Label label = Label.label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( label.name() ), array( "prop" ) ) ).close();
            tx.success();
        }
        long nodeIdA;
        long nodeIdB;
        try ( Transaction tx = db.beginTx() )
        {
            awaitIndexesOnline();
            Node nodeA = db.createNode( label );
            nodeA.setProperty( "prop", "value" );
            nodeIdA = nodeA.getId();
            Node nodeB = db.createNode( label );
            nodeB.setProperty( "prop", "value" );
            nodeIdB = nodeB.getId();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( nodeIdA ).delete();
            db.getNodeById( nodeIdB ).delete();
            try ( Result result = db.execute( format( QUERY_NODES, "nodes", "value" ) ) )
            {
                assertThat( result.stream().count(), is( 0L ) );
            }
            tx.success();
        }
    }

    @Test
    public void queryResultsMustNotIncludeRelationshipsDeletedInThisTransaction()
    {
        db = createDatabase();
        RelationshipType relType = RelationshipType.withName( "REL" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( relType.name() ), array( "prop" ) ) ).close();
            tx.success();
        }
        long relIdA;
        long relIdB;
        try ( Transaction tx = db.beginTx() )
        {
            awaitIndexesOnline();
            Node node = db.createNode();
            Relationship relA = node.createRelationshipTo( node, relType );
            relA.setProperty( "prop", "value" );
            relIdA = relA.getId();
            Relationship relB = node.createRelationshipTo( node, relType );
            relB.setProperty( "prop", "value" );
            relIdB = relB.getId();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.getRelationshipById( relIdA ).delete();
            db.getRelationshipById( relIdB ).delete();
            try ( Result result = db.execute( format( QUERY_RELS, "rels", "value" ) ) )
            {
                assertThat( result.stream().count(), is( 0L ) );
            }
            tx.success();
        }
    }

    // todo must include things added in same transaction
    // todo must include things modified in same transaction
    // todo dropping/creating indexes?

    private GraphDatabaseAPI createDatabase()
    {
        return (GraphDatabaseAPI) cleanup.add( builder.newGraphDatabase() );
    }

    private void awaitIndexesOnline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    static void assertQueryFindsIds( GraphDatabaseService db, String index, String query, long... ids )
    {
        try ( Transaction tx = db.beginTx() )
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
            tx.success();
        }
    }

    static void assertQueryFindsIds( GraphDatabaseService db, LongFunction<Entity> getEntity, String index, String query, LongHashSet ids )
    {
        ids = new LongHashSet( ids ); // Create a defensive copy, because we're going to modify this instance.
        long[] expectedIds = ids.toArray();
        MutableLongSet actualIds = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
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
            tx.success();
        }
    }

    private static void failQuery( LongFunction<Entity> getEntity, String index, String query, MutableLongSet ids, long[] expectedIds, MutableLongSet actualIds,
            String msg )
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

    private List<Value> generateRandomNonStringValues( int valuesToGenerate )
    {
        RandomValues generator = RandomValues.create();
        List<Value> values = new ArrayList<>( valuesToGenerate );
        for ( int i = 0; i < valuesToGenerate; i++ )
        {
            Value value;
            do
            {
                value = generator.nextValue();
            }
            while ( value.valueGroup() == ValueGroup.TEXT );
            values.add( value );
        }
        return values;
    }

    private String quoteValueForQuery( Value value )
    {
        return QueryParserUtil.escape( value.prettyPrint() ).replace( "\\", "\\\\" ).replace( "\"", "\\\"" );
    }
}
