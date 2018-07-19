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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.AWAIT_POPULATION;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.NODE_CREATE;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.RELATIONSHIP_CREATE;
import static com.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.array;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

public class FulltextIndexConsistencyCheckIT
{
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
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.storeDir() );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, "false" );
    }

    private GraphDatabaseService createDatabase()
    {
        return cleanup.add( builder.newGraphDatabase() );
    }

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
            for ( int i = 1; i < labels.length; i++ )
            {
                db.execute( format( AWAIT_POPULATION, "nodes" + i ) ).close();
            }
            for ( int i = 1; i < relTypes.length; i++ )
            {
                db.execute( format( AWAIT_POPULATION, "rels" + i ) ).close();
            }
            tx.success();
        }

        db.shutdown();

        Config config = Config.defaults();
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService( new Date() );
        ConsistencyFlags checkConsistencyConfig = new ConsistencyFlags( config );
        ConsistencyCheckService.Result result = consistencyCheckService.runFullConsistencyCheck( testDirectory.databaseDir(), config,
                ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), true, checkConsistencyConfig );
        assertTrue( result.isSuccessful() );
    }
}
