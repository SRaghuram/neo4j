/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.Values;

import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.NODE_CREATE;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.RELATIONSHIP_CREATE;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.array;
import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterables.first;

public class FulltextIndexConsistencyCheckIT
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    private final CleanupRule cleanup = new CleanupRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( fs ).around( testDirectory ).around( expectedException ).around( cleanup );

    private GraphDatabaseBuilder builder;

    @Before
    public void before()
    {
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.databaseDir() );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, "false" );
    }

    private GraphDatabaseService createDatabase()
    {
        return cleanup.add( builder.newGraphDatabase() );
    }

    private ConsistencyCheckService.Result checkConsistency() throws ConsistencyCheckIncompleteException
    {
        Config config = Config.defaults();
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService( new Date() );
        ConsistencyFlags checkConsistencyConfig = new ConsistencyFlags( config );
        return consistencyCheckService.runFullConsistencyCheck( testDirectory.databaseLayout(), config,
                ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), true, checkConsistencyConfig );
    }

    private StoreIndexDescriptor getIndexDescriptor( IndexDefinition definition )
    {
        StoreIndexDescriptor indexDescriptor;
        IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) definition;
        indexDescriptor = (StoreIndexDescriptor) indexDefinition.getIndexReference();
        return indexDescriptor;
    }

    private IndexingService getIndexingService( GraphDatabaseService db )
    {
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        DependencyResolver dependencyResolver = api.getDependencyResolver();
        return dependencyResolver.resolveDependency( IndexingService.class, DependencyResolver.SelectionStrategy.ONLY );
    }

    private void assertIsConsistent( ConsistencyCheckService.Result result ) throws IOException
    {
        if ( !result.isSuccessful() )
        {
            printReport( result );
            fail( "Expected consistency check to be successful." );
        }
    }

    private void printReport( ConsistencyCheckService.Result result ) throws IOException
    {
        Files.readAllLines( result.reportFile().toPath() ).forEach( System.err::println );
    }

    @Test
    public void mustBeAbleToConsistencyCheckEmptyDatabaseWithFulltextIndexingEnabled() throws Exception
    {
        createDatabase().shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexWithOneLabelAndOneProperty() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "Label" ), array( "prop" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            db.createNode( Label.label( "Label" ) ).setProperty( "prop", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexWithOneLabelAndMultipleProperties() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "Label" ), array( "p1", "p2" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node node = db.createNode( Label.label( "Label" ) );
            node.setProperty( "p1", "value" );
            node.setProperty( "p2", "value" );
            db.createNode( Label.label( "Label" ) ).setProperty( "p1", "value" );
            db.createNode( Label.label( "Label" ) ).setProperty( "p2", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexWithMultipleLabelsAndOneProperty() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "L1", "L2" ), array( "prop" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            db.createNode( Label.label( "L1" ), Label.label( "L2" ) ).setProperty( "prop", "value" );
            db.createNode( Label.label( "L2" ) ).setProperty( "prop", "value" );
            db.createNode( Label.label( "L1" ) ).setProperty( "prop", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexWithMultipleLabelsAndMultipleProperties() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "L1", "L2" ), array( "p1", "p2" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node n1 = db.createNode( Label.label( "L1" ), Label.label( "L2" ) );
            n1.setProperty( "p1", "value" );
            n1.setProperty( "p2", "value" );
            Node n2 = db.createNode( Label.label( "L1" ), Label.label( "L2" ) );
            n2.setProperty( "p1", "value" );
            Node n3 = db.createNode( Label.label( "L1" ), Label.label( "L2" ) );
            n3.setProperty( "p2", "value" );
            Node n4 = db.createNode( Label.label( "L1" ) );
            n4.setProperty( "p1", "value" );
            n4.setProperty( "p2", "value" );
            Node n5 = db.createNode( Label.label( "L1" ) );
            n5.setProperty( "p1", "value" );
            Node n6 = db.createNode( Label.label( "L1" ) );
            n6.setProperty( "p2", "value" );
            Node n7 = db.createNode( Label.label( "L2" ) );
            n7.setProperty( "p1", "value" );
            n7.setProperty( "p2", "value" );
            Node n8 = db.createNode( Label.label( "L2" ) );
            n8.setProperty( "p1", "value" );
            Node n9 = db.createNode( Label.label( "L2" ) );
            n9.setProperty( "p2", "value" );
            db.createNode( Label.label( "L2" ) ).setProperty( "p1", "value" );
            db.createNode( Label.label( "L2" ) ).setProperty( "p2", "value" );
            db.createNode( Label.label( "L1" ) ).setProperty( "p1", "value" );
            db.createNode( Label.label( "L1" ) ).setProperty( "p2", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }
    // todo must be able to consistency check relationship index with one label and one property
    // todo must be able to consistency check relationship index with one label and multiple properties
    // todo must be able to consistency check relationship index with multiple labels and one property
    // todo must be able to consistency check relationship index with multiple labels and multiple properties
    // todo ... same as above, but including property value types that are not indexed by the fulltext index.

    @Test
    public void consistencyCheckerMustBeAbleToRunOnStoreWithFulltextIndexes() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        Label[] labels = IntStream.range( 1, 7 ).mapToObj( i -> Label.label( "LABEL" + i ) ).toArray( Label[]::new );
        RelationshipType[] relTypes = IntStream.range( 1, 5 ).mapToObj( i -> RelationshipType.withName( "REL" + i ) ).toArray( RelationshipType[]::new );
        String[] propertyKeys = IntStream.range( 1, 7 ).mapToObj( i -> "PROP" + i ).toArray( String[]::new );

        try ( Transaction tx = db.beginTx() )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int nodeCount = 1000;
            List<Node> nodes = new ArrayList<>( nodeCount );
            for ( int i = 0; i < nodeCount; i++ )
            {
                Label[] nodeLabels = rng.ints( rng.nextInt( labels.length ), 0, labels.length ).distinct().mapToObj( x -> labels[x] ).toArray( Label[]::new );
                Node node = db.createNode( nodeLabels );
                Stream.of( propertyKeys ).forEach( p -> node.setProperty( p, p ) );
                nodes.add( node );
                int localRelCount = Math.min( nodes.size(), 5 );
                rng.ints( localRelCount, 0, localRelCount ).distinct()
                        .mapToObj( x -> node.createRelationshipTo( nodes.get( x ), relTypes[rng.nextInt( relTypes.length )] ) )
                        .forEach( r -> Stream.of( propertyKeys ).forEach( p -> r.setProperty( p, p ) ) );
            }
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 1; i < labels.length; i++ )
            {
                db.execute( format( NODE_CREATE, "nodes" + i,
                        array( Arrays.stream( labels ).limit( i ).map( Label::name ).toArray( String[]::new ) ),
                        array( Arrays.copyOf( propertyKeys, i ) ) ) ).close();
            }
            for ( int i = 1; i < relTypes.length; i++ )
            {
                db.execute( format( RELATIONSHIP_CREATE, "rels" + i,
                        array( Arrays.stream( relTypes ).limit( i ).map( RelationshipType::name ).toArray( String[]::new ) ),
                        array( Arrays.copyOf( propertyKeys, i ) ) ) ).close();
            }
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }

        db.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertIsConsistent( result );
    }

    @Test
    public void mustDiscoverNodeInStoreMissingFromIndex() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "Label" ), array( "prop" ) ) ).close();
            tx.success();
        }
        StoreIndexDescriptor indexDescriptor;
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            indexDescriptor = getIndexDescriptor( first( db.schema().getIndexes() ) );
            Node node = db.createNode( Label.label( "Label" ) );
            node.setProperty( "prop", "value" );
            nodeId = node.getId();
            tx.success();
        }
        IndexingService indexes = getIndexingService( db );
        IndexProxy indexProxy = indexes.getIndexProxy( indexDescriptor.schema() );
        try ( IndexUpdater updater = indexProxy.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( IndexEntryUpdate.remove( nodeId, indexDescriptor, Values.stringValue( "value" ) ) );
        }

        db.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse( result.isSuccessful() );
    }
    // todo must discover node in store missing from index
    // todo must discover node in index missing from store
    // todo must discover relationship in store missing from index
    // todo must discover relationship in index missing from store
}
