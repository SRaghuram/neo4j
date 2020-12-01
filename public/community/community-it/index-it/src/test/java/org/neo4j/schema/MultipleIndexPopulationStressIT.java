/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.GeneratingInputIterator;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.ParallelBatchImporter;
import org.neo4j.internal.batchimport.RandomsStates;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.PropertySizeCalculator;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.helpers.TimeUtil;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.values.storable.RandomValues;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.index_population_queue_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.internal.batchimport.GeneratingInputIterator.EMPTY_ITERABLE;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.internal.batchimport.input.Input.knownEstimates;
import static org.neo4j.internal.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * Idea is to test a {@link MultipleIndexPopulator} with a bunch of indexes, some of which can fail randomly.
 * Also updates are randomly streaming in during population. In the end all the indexes should have been populated
 * with correct data.
 */
@ExtendWith( RandomExtension.class )
@TestDirectoryExtension
class MultipleIndexPopulationStressIT
{
    private static final String[] TOKENS = new String[]{"One", "Two", "Three", "Four"};
    private ExecutorService executor;

    @Inject
    private RandomRule random;

    @Inject
    private TestDirectory directory;

    @Inject
    private DefaultFileSystemAbstraction fileSystemAbstraction;

    @AfterEach
    public void tearDown()
    {
        if ( executor != null )
        {
            executor.shutdown();
        }
    }

    @Test
    public void populateMultipleIndexWithSeveralNodesMultiThreaded() throws Exception
    {
        prepareAndRunTest( 10, TimeUnit.SECONDS.toMillis( 5 ), index_population_queue_threshold.defaultValue() );
    }

    @Test
    public void shouldPopulateMultipleIndexPopulatorsUnderStressMultiThreaded() throws Exception
    {
        int concurrentUpdatesQueueFlushThreshold = random.nextInt( 100, 5000 );
        readConfigAndRunTest( concurrentUpdatesQueueFlushThreshold );
    }

    private void readConfigAndRunTest( int concurrentUpdatesQueueFlushThreshold ) throws Exception
    {
        // GIVEN a database with random data in it
        int nodeCount = (int) SettingValueParsers.parseLongWithUnit( System.getProperty( getClass().getName() + ".nodes", "200k" ) );
        long duration = TimeUtil.parseTimeMillis.apply( System.getProperty( getClass().getName() + ".duration", "5s" ) );
        prepareAndRunTest( nodeCount, duration, concurrentUpdatesQueueFlushThreshold );
    }

    private void prepareAndRunTest( int nodeCount, long durationMillis, int concurrentUpdatesQueueFlushThreshold ) throws Exception
    {
        createRandomData( nodeCount );
        long endTime = currentTimeMillis() + durationMillis;

        // WHEN/THEN run tests for at least the specified durationMillis
        while ( currentTimeMillis() < endTime )
        {
            runTest( nodeCount, concurrentUpdatesQueueFlushThreshold );
        }
    }

    private void runTest( int nodeCount, int concurrentUpdatesQueueFlushThreshold ) throws Exception
    {
        // WHEN creating the indexes under stressful updates
        populateDbAndIndexes( nodeCount );
        ConsistencyCheckService cc = new ConsistencyCheckService();
        Config config = Config.newBuilder()
                .set( neo4j_home, directory.homePath() )
                .set( GraphDatabaseSettings.pagecache_memory, "8m" )
                .set( index_population_queue_threshold, concurrentUpdatesQueueFlushThreshold )
                .build();
        Result result = cc.runFullConsistencyCheck( DatabaseLayout.of( config ),
                config,
                NONE, NullLogProvider.getInstance(), false );
        assertTrue( result.isSuccessful() );
        dropIndexes();
    }

    private void populateDbAndIndexes( int nodeCount ) throws InterruptedException
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( directory.homePath() ).build();
        final GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            createIndexes( db );
            final AtomicBoolean end = new AtomicBoolean();
            executor = Executors.newCachedThreadPool();
            for ( int i = 0; i < 10; i++ )
            {
                executor.submit( () ->
                {
                    RandomValues randomValues = RandomValues.create();
                    while ( !end.get() )
                    {
                        changeRandomNode( db, nodeCount, randomValues );
                    }
                } );
            }

            while ( !indexesAreOnline( db ) )
            {
                Thread.sleep( 100 );
            }
            end.set( true );
            executor.shutdown();
            executor.awaitTermination( 10, SECONDS );
            executor = null;
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private void dropIndexes()
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( directory.homePath() )
                        .setConfig( GraphDatabaseSettings.pagecache_memory, "8m" )
                        .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : tx.schema().getIndexes() )
            {
                index.drop();
            }
            tx.commit();
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private boolean indexesAreOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : tx.schema().getIndexes() )
            {
                switch ( tx.schema().getIndexState( index ) )
                {
                case ONLINE:
                    break; // Good
                case POPULATING:
                    return false; // Still populating
                case FAILED:
                    fail( index + " entered failed state: " + tx.schema().getIndexFailure( index ) );
                default:
                    throw new UnsupportedOperationException();
                }
            }
            tx.commit();
        }
        return true;
    }

    /**
     * Create a bunch of indexes in a single transaction. This will have all the indexes being built
     * using a single store scan... and this is the gist of what we're testing.
     */
    private void createIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( String label : random.selection( TOKENS, 3, 3, false ) )
            {
                for ( String propertyKey : random.selection( TOKENS, 3, 3, false ) )
                {
                    tx.schema().indexFor( Label.label( label ) ).on( propertyKey ).create();
                }
            }
            tx.commit();
        }
    }

    private void changeRandomNode( GraphDatabaseService db, int nodeCount, RandomValues random )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = random.nextInt( nodeCount );
            Node node = tx.getNodeById( nodeId );
            Object[] keys = Iterables.asCollection( node.getPropertyKeys() ).toArray();
            String key = (String) random.among( keys );
            if ( random.nextFloat() < 0.1 )
            {   // REMOVE
                node.removeProperty( key );
            }
            else
            {   // CHANGE
                node.setProperty( key, random.nextValue().asObject() );
            }
            tx.commit();
        }
        catch ( NotFoundException e )
        {   // It's OK, it happens if some other thread deleted that property in between us reading it and
            // removing or setting it
        }
    }

    private void createRandomData( int count ) throws Exception
    {
        Config config = Config.defaults( neo4j_home, directory.homePath() );
        RecordFormats recordFormats = RecordFormatSelector.selectForConfig( config, NullLogProvider.getInstance() );
        try ( RandomDataInput input = new RandomDataInput( count );
              JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
        {
            DatabaseLayout layout = Neo4jLayout.of( directory.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            BatchImporter importer = new ParallelBatchImporter(
                    layout, fileSystemAbstraction, null, PageCacheTracer.NULL, DEFAULT, NullLogService.getInstance(),
                    ExecutionMonitor.INVISIBLE, EMPTY, config, recordFormats, NO_MONITOR, jobScheduler, Collector.EMPTY,
                    TransactionLogInitializer.getLogFilesInitializer(), INSTANCE );
            importer.doImport( input );
        }
    }

    private class RandomNodeGenerator extends GeneratingInputIterator<RandomValues>
    {
        RandomNodeGenerator( int count, Generator<RandomValues> randomsGenerator )
        {
            super( count, 1_000, new RandomsStates( random.seed() ), randomsGenerator, 0 );
        }
    }

    private class RandomDataInput implements Input, AutoCloseable
    {
        private final int count;
        private final BadCollector badCollector;

        RandomDataInput( int count )
        {
            this.count = count;
            this.badCollector = createBadCollector();
        }

        @Override
        public InputIterable relationships( Collector badCollector )
        {
            return EMPTY_ITERABLE;
        }

        @Override
        public InputIterable nodes( Collector badCollector )
        {
            return () -> new RandomNodeGenerator( count, ( state, visitor, id ) -> {
                String[] keys = random.randomValues().selection( TOKENS, 1, TOKENS.length, false );
                for ( String key : keys )
                {
                    visitor.property( key, random.nextValueAsObject() );
                }
                visitor.labels( random.selection( TOKENS, 1, TOKENS.length, false ) );
            } );
        }

        @Override
        public IdType idType()
        {
            return IdType.ACTUAL;
        }

        @Override
        public ReadableGroups groups()
        {
            return ReadableGroups.EMPTY;
        }

        private BadCollector createBadCollector()
        {
            try
            {
                return new BadCollector( fileSystemAbstraction.openAsOutputStream( directory.homePath().resolve( "bad" ), false ), 0, 0 );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public Estimates calculateEstimates( PropertySizeCalculator valueSizeCalculator )
        {
            return knownEstimates( count, 0, count * TOKENS.length / 2, 0, count * TOKENS.length / 2 * Long.BYTES, 0, 0 );
        }

        @Override
        public void close()
        {
            badCollector.close();
        }
    }
}
