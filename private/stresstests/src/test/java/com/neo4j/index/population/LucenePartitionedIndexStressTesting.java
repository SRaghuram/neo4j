/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.index.population;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static com.neo4j.helper.StressTestingHelper.fromEnv;
import static org.apache.commons.lang3.SystemUtils.JAVA_IO_TMPDIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class LucenePartitionedIndexStressTesting
{
    private static final String LABEL = "label";
    private static final String PROPERTY_PREFIX = "property";
    private static final String UNIQUE_PROPERTY_PREFIX = "uniqueProperty";

    private static final int NUMBER_OF_PROPERTIES = 2;

    private static final int NUMBER_OF_POPULATORS =
            Integer.parseInt( fromEnv( "LUCENE_INDEX_NUMBER_OF_POPULATORS",
                    String.valueOf( Runtime.getRuntime().availableProcessors() - 1 ) ) );
    private static final int BATCH_SIZE = Integer.parseInt( fromEnv( "LUCENE_INDEX_POPULATION_BATCH_SIZE",
            String.valueOf( 10000 ) ) );

    private static final long NUMBER_OF_NODES = Long.parseLong( fromEnv(
            "LUCENE_PARTITIONED_INDEX_NUMBER_OF_NODES", String.valueOf( 100000 ) ) );
    private static final String WORK_DIRECTORY =
            fromEnv( "LUCENE_PARTITIONED_INDEX_WORKING_DIRECTORY", JAVA_IO_TMPDIR );
    private static final int WAIT_DURATION_MINUTES = Integer.parseInt( fromEnv(
            "LUCENE_PARTITIONED_INDEX_WAIT_TILL_ONLINE", String.valueOf( 30 ) ) );

    private ExecutorService populators;
    private GraphDatabaseService db;
    private Path storeDir;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() throws IOException
    {
        storeDir = prepareStoreDir();
        System.out.println( String.format( "Starting database at: %s", storeDir ) );

        populators = Executors.newFixedThreadPool( NUMBER_OF_POPULATORS );
        managementService = new TestDatabaseManagementServiceBuilder( storeDir ).build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown() throws IOException
    {
        managementService.shutdown();
        populators.shutdown();
        FileUtils.deletePathRecursively( storeDir );
    }

    @Test
    void indexCreationStressTest() throws Exception
    {
        createIndexes();
        createUniqueIndexes();
        PopulationResult populationResult = populateDatabase();
        findLastTrackedNodesByLabelAndProperties( db, populationResult );
        dropAllIndexes();

        createUniqueIndexes();
        createIndexes();
        findLastTrackedNodesByLabelAndProperties( db, populationResult );
    }

    private void dropAllIndexes()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Schema schema = transaction.schema();
            schema.getConstraints().forEach( ConstraintDefinition::drop );
            schema.getIndexes().forEach( IndexDefinition::drop );
            transaction.commit();
        }
    }

    private void createIndexes()
    {
        createIndexes( false );
    }

    private void createUniqueIndexes()
    {
        createIndexes( true );
    }

    private void createIndexes( boolean unique )
    {
        System.out.println( String.format( "Creating %d%s indexes.", NUMBER_OF_PROPERTIES, unique ? " unique" : "" ) );
        long creationStart = System.nanoTime();
        createAndWaitForIndexes( unique );
        System.out.println( String.format( "%d%s indexes created.", NUMBER_OF_PROPERTIES, unique ? " unique" : "" ) );
        System.out.println( "Creation took: " + TimeUnit.NANOSECONDS.toMillis( System.nanoTime() -
                                                                               creationStart ) + " ms." );
    }

    private PopulationResult populateDatabase() throws ExecutionException, InterruptedException
    {
        System.out.println( "Starting database population." );
        long populationStart = System.nanoTime();
        PopulationResult populationResult = populateDb( db );

        System.out.println( "Database population completed. Inserted " + populationResult.numberOfNodes + " nodes." );
        System.out.println( "Population took: " + TimeUnit.NANOSECONDS.toMillis( System.nanoTime() -
                                                                                 populationStart ) + " ms." );
        return populationResult;
    }

    private void findLastTrackedNodesByLabelAndProperties( GraphDatabaseService db, PopulationResult populationResult )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeByUniqueStringProperty = tx.findNode( Label.label( LABEL ), getUniqueStringProperty(),
                    populationResult.maxPropertyId + "" );
            Node nodeByStringProperty = tx.findNode( Label.label( LABEL ), getStringProperty(),
                    populationResult.maxPropertyId + "" );
            assertNotNull( nodeByStringProperty, "Should find last inserted node" );
            assertEquals( nodeByStringProperty,
                    nodeByUniqueStringProperty, "Both nodes should be the same last inserted node" );

            Node nodeByUniqueLongProperty = tx.findNode( Label.label( LABEL ), getUniqueLongProperty(),
                    populationResult.maxPropertyId );
            Node nodeByLongProperty = tx.findNode( Label.label( LABEL ), getLongProperty(),
                    populationResult.maxPropertyId );
            assertNotNull( nodeByLongProperty, "Should find last inserted node" );
            assertEquals( nodeByLongProperty,
                    nodeByUniqueLongProperty, "Both nodes should be the same last inserted node" );

        }
    }

    private static Path prepareStoreDir() throws IOException
    {
        Path storeDirectory = Paths.get( WORK_DIRECTORY ).resolve( Paths.get( "storeDir" ) );
        FileUtils.deletePathRecursively( storeDirectory );
        return storeDirectory;
    }

    private PopulationResult populateDb( GraphDatabaseService db ) throws ExecutionException, InterruptedException
    {
        AtomicLong nodesCounter = new AtomicLong();

        List<Future<Long>> futures = new ArrayList<>( NUMBER_OF_POPULATORS );
        for ( int i = 0; i < NUMBER_OF_POPULATORS; i++ )
        {
            futures.add( populators.submit( new Populator( i, NUMBER_OF_POPULATORS, db, nodesCounter ) ) );
        }

        long maxPropertyId = 0;
        for ( Future<Long> future : futures )
        {
            maxPropertyId = Math.max( maxPropertyId, future.get() );
        }
        return new PopulationResult( maxPropertyId, nodesCounter.get() );
    }

    private void createAndWaitForIndexes( boolean unique )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            for ( int i = 0; i < NUMBER_OF_PROPERTIES; i++ )
            {
                if ( unique )
                {
                    createUniqueConstraint( transaction, i );
                }
                else
                {
                    createIndex( transaction, i );
                }
            }
            transaction.commit();
        }
        awaitIndexesOnline( db );
    }

    private void createUniqueConstraint( Transaction tx, int index )
    {
        tx.schema().constraintFor( Label.label( LABEL ) ).assertPropertyIsUnique( UNIQUE_PROPERTY_PREFIX + index )
                .create();
    }

    private void createIndex( Transaction tx, int index )
    {
        tx.schema().indexFor( Label.label( LABEL ) ).on( PROPERTY_PREFIX + index ).create();
    }

    private void awaitIndexesOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Schema schema = tx.schema();
            schema.awaitIndexesOnline( WAIT_DURATION_MINUTES, TimeUnit.MINUTES );
        }
    }

    private static String getLongProperty()
    {
        return PROPERTY_PREFIX + 1;
    }

    private static String getStringProperty()
    {
        return PROPERTY_PREFIX + 0;
    }

    private static String getUniqueLongProperty()
    {
        return UNIQUE_PROPERTY_PREFIX + 1;
    }

    private static String getUniqueStringProperty()
    {
        return UNIQUE_PROPERTY_PREFIX + 0;
    }

    private static class SequentialStringSupplier implements Supplier<String>
    {
        private final int step;
        long value;

        SequentialStringSupplier( int populatorNumber, int step )
        {
            this.value = populatorNumber;
            this.step = step;
        }

        @Override
        public String get()
        {
            value += step;
            return value + "";
        }
    }

    private static class SequentialLongSupplier implements LongSupplier
    {
        long value;
        private final int step;

        SequentialLongSupplier( int populatorNumber, int step )
        {
            value = populatorNumber;
            this.step = step;
        }

        @Override
        public long getAsLong()
        {
            value += step;
            return value;
        }
    }

    private static class Populator implements Callable<Long>
    {
        private final int populatorNumber;
        private final int step;
        private final GraphDatabaseService db;
        private final AtomicLong nodesCounter;

        Populator( int populatorNumber, int step, GraphDatabaseService db, AtomicLong nodesCounter )
        {
            this.populatorNumber = populatorNumber;
            this.step = step;
            this.db = db;
            this.nodesCounter = nodesCounter;
        }

        @Override
        public Long call()
        {
            SequentialLongSupplier longSupplier = new SequentialLongSupplier( populatorNumber, step );
            SequentialStringSupplier stringSupplier = new SequentialStringSupplier( populatorNumber, step );

            while ( nodesCounter.get() < NUMBER_OF_NODES )
            {
                long nodesInTotal = nodesCounter.addAndGet( insertBatchNodes( db, stringSupplier, longSupplier ) );
                if ( nodesInTotal % 1_000_000 == 0 )
                {
                    System.out.println( "Inserted " + nodesInTotal + " nodes." );
                }
            }
            return longSupplier.value;
        }

        private int insertBatchNodes( GraphDatabaseService db, Supplier<String> stringValueSupplier,
                LongSupplier longSupplier )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                for ( int i = 0; i < BATCH_SIZE; i++ )
                {
                    Node node = transaction.createNode( Label.label( LABEL ) );

                    String stringValue = stringValueSupplier.get();
                    long longValue = longSupplier.getAsLong();

                    node.setProperty( getStringProperty(), stringValue );
                    node.setProperty( getLongProperty(), longValue );

                    node.setProperty( getUniqueStringProperty(), stringValue );
                    node.setProperty( getUniqueLongProperty(), longValue );
                }
                transaction.commit();
            }
            return BATCH_SIZE;
        }
    }

    private static class PopulationResult
    {
        private final long maxPropertyId;
        private final long numberOfNodes;

        PopulationResult( long maxPropertyId, long numberOfNodes )
        {
            this.maxPropertyId = maxPropertyId;
            this.numberOfNodes = numberOfNodes;
        }
    }
}
