/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom.surface;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.TestDirectory;

@Ignore( "These are rudimentary benchmarks, but implemented via the jUnit framework to make them easy to run " +
         "from an IDE." )
public class BloomFulltextIndexBenchmarks
{
    private static final String[] WORDS =
            ("dui nunc mattis enim ut tellus elementum sagittis vitae et leo duis ut diam quam nulla porttitor " +
             "massa id neque aliquam vestibulum morbi blandit cursus risus at ultrices mi tempus imperdiet nulla " +
             "malesuada pellentesque elit eget gravida cum sociis natoque penatibus et magnis dis parturient " +
             "montes nascetur ridiculus mus mauris").split( " " );

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private GraphDatabaseFactory factory;
    private GraphDatabaseService db;

    private void createTestGraphDatabaseFactory()
    {
        factory = new GraphDatabaseFactory();
    }

    private void registerBloomProcedures() throws KernelException
    {
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( Procedures.class ).registerProcedure( BloomProcedures.class );
    }

    private static void clearAndCreateRandomSentence( ThreadLocalRandom rng, StringBuilder sb )
    {
        sb.setLength( 0 );
        int wordCount = rng.nextInt( 3, 7 );
        for ( int k = 0; k < wordCount; k++ )
        {
            sb.append( WORDS[rng.nextInt( WORDS.length )] ).append( ' ' );
        }
        sb.setLength( sb.length() - 1 );
    }

    private void setupDb() throws KernelException
    {
        createTestGraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( BloomFulltextConfig.bloom_enabled, "true" );
        db = builder.newGraphDatabase();
        registerBloomProcedures();
    }

    @Test
    public void fiveHundredThousandOnlineUpdates() throws Exception
    {
        setupDb();
        db.execute( "call db.fulltext.bloomFulltextSetPropertyKeys([\"prop\"])" );

        int trials = 50;
        int threadCount = 10;
        int updatesPerThread = 1000;
        int updatesPerIteration = 10;
        int iterationsPerThread = updatesPerThread / updatesPerIteration;

        Runnable work = () ->
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            StringBuilder sb = new StringBuilder();
            for ( int i = 0; i < iterationsPerThread; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    for ( int j = 0; j < updatesPerIteration; j++ )
                    {
                        clearAndCreateRandomSentence( rng, sb );
                        db.createNode().setProperty( "prop", sb.toString() );
                    }
                    tx.success();
                }
            }
        };

        for ( int i = 0; i < trials; i++ )
        {
            long startMillis = System.currentTimeMillis();
            Thread[] threads = new Thread[threadCount];
            for ( int j = 0; j < threadCount; j++ )
            {
                threads[j] = new Thread( work );
            }
            for ( Thread thread : threads )
            {
                thread.start();
            }
            for ( Thread thread : threads )
            {
                thread.join();
            }
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            System.out.printf( "online update elapsed: %s ms.%n", elapsedMillis );
        }
    }

    @Test
    public void fiveHundredThousandNodesForPopulation() throws Exception
    {
        // First create the data
        createTestGraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        db = builder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            StringBuilder sb = new StringBuilder();
            for ( int i = 0; i < 500_000; i++ )
            {
                clearAndCreateRandomSentence( rng, sb );
                db.createNode().setProperty( "prop", sb.toString() );
            }
            tx.success();
        }
        db.shutdown();

        // Then measure startup performance
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( BloomFulltextConfig.bloom_enabled, "true" );

        db = builder.newGraphDatabase();
        registerBloomProcedures();
        for ( int i = 0; i < 50; i++ )
        {
            long startMillis = System.currentTimeMillis();
            db.execute( "call db.fulltext.bloomFulltextSetPropertyKeys([\"prop\"])" );
            db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            System.out.printf( "startup populate elapsed: %s ms.%n", elapsedMillis );
            db.execute( "call db.fulltext.bloomFulltextSetPropertyKeys([])" );
            db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
        }
        db.shutdown();
        db = null;
    }

    @After
    public void after()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }
}
