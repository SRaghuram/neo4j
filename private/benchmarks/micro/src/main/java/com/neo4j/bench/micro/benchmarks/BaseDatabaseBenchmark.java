/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.jmh.api.BaseBenchmark;
import com.neo4j.bench.jmh.api.RunnerParams;
import com.neo4j.bench.micro.data.Augmenterizer;
import com.neo4j.bench.micro.data.Augmenterizer.NullAugmenterizer;
import com.neo4j.bench.micro.data.DataGenerator;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.ManagedStore;
import com.neo4j.bench.micro.data.Stores;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Path;
import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;

/**
 * Benchmark classes that extend this class will operate with the following life-cycle (note that there is no <code>'setup()'</code> method):
 * <p>
 * <ol>
 * <li>{@link BaseDatabaseBenchmark#getConfig()}</li>
 * <li>Store Generation <p>
 *     Base store generation is performed, using the <code>DataGeneratorConfig</code> returned by <code>getConfig()</code> </li>
 * <li>Store Augmentation <p>
 *     The <code>Augmenterizer</code> returned by <code>augmentDataGeneration()</code> is now invoked, to perform additional store generation</li>
 * <li>{@link BaseDatabaseBenchmark#afterDataGeneration()} <p>
 *     After store generation & before starting of the database, <code>afterDataGeneration()</code> will be called. <p>
 *     Override this callback to mess with the generated store, or to prevent the database from starting.</li>
 * <li>{@link BaseDatabaseBenchmark#afterDatabaseStart(DataGeneratorConfig)} <p>
 *     After the database is started <code>afterDatabaseStart()</code> will be called, <p>
 *     but only if {@link BaseDatabaseBenchmark#afterDataGeneration()} returns {@link StartDatabaseInstruction#START_DB}. <p>
 *     Note that if {@link StartDatabaseInstruction#DO_NOT_START_DB} is returned this callback will never be called.</li>
 * <li>Benchmark Execution</li>
 * <li>{@link BaseDatabaseBenchmark#benchmarkTearDown()}</li> <p>
 *     After benchmark completes teardown will be called, all teardown logic should live in that method.
 * </ol>
 */
@State( Scope.Benchmark )
public abstract class BaseDatabaseBenchmark extends BaseBenchmark
{
    @Param( {} )
    public String baseNeo4jConfig;

    public enum StartDatabaseInstruction
    {
        START_DB,
        DO_NOT_START_DB
    }

    protected ManagedStore managedStore;

    protected GraphDatabaseService db()
    {
        return managedStore.db();
    }

    protected GraphDatabaseService systemDb()
    {
        return managedStore.systemDb();
    }

    protected Store store()
    {
        return managedStore.store();
    }

    @Override
    protected final void onSetup( BenchmarkGroup group,
                                  Benchmark benchmark,
                                  RunnerParams runnerParams,
                                  BenchmarkParams benchmarkParams,
                                  ForkDirectory forkDirectory )
    {
        Stores stores = new Stores( runnerParams.workDir() );

        DataGeneratorConfig baseBenchmarkGeneratorConfig = getConfig();
        Neo4jConfig neo4jConfig = Neo4jConfig.fromJson( this.baseNeo4jConfig )
                                             .mergeWith( baseBenchmarkGeneratorConfig.neo4jConfig() );

        Path neo4jConfigFile = forkDirectory.create( "neo4j.conf" );
        System.out.println( "\nWriting Neo4j config to: " + neo4jConfigFile.toAbsolutePath() );
        Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );

        Augmenterizer augmenterizer = augmentDataGeneration();

        DataGeneratorConfig finalGeneratorConfig = DataGeneratorConfigBuilder
                .from( baseBenchmarkGeneratorConfig )
                .withNeo4jConfig( neo4jConfig )
                .withRngSeed( DataGenerator.DEFAULT_RNG_SEED )
                .augmentedBy( augmenterizer.augmentKey( FullBenchmarkName.from( group, benchmark ) ) )
                .build();
        Stores.StoreAndConfig storeAndConfig = stores.prepareDb(
                finalGeneratorConfig,
                group,
                benchmark,
                augmenterizer,
                neo4jConfigFile,
                benchmarkParams.getThreads() );

        managedStore = new ManagedStore( finalGeneratorConfig, storeAndConfig );

        if ( afterDataGeneration().equals( StartDatabaseInstruction.START_DB ) )
        {
            managedStore.startDb();
            afterDatabaseStart( finalGeneratorConfig );
        }
        else
        {
            System.out.println( "Database will not be started!" );
        }
    }

    @Override
    protected final void onTearDown() throws Exception
    {
        managedStore.tearDownDb();
        benchmarkTearDown();
    }

    /**
     * augment (verb): to make (something) greater by adding to it.
     * <p>
     * This method allows a benchmark to perform additional data generation, that augments the already generated store.
     * <p>
     * Method is called after standard store generation, but before the store is cached.
     * Meaning augmentations will be cached along with the original store, and be available to all forks.
     *
     * @return augmenting method
     */
    protected Augmenterizer augmentDataGeneration()
    {
        return new NullAugmenterizer();
    }

    /**
     * Called after store generation & before starting of database.
     *
     * @return value specifying whether store should be started.
     */
    protected StartDatabaseInstruction afterDataGeneration()
    {
        return StartDatabaseInstruction.START_DB;
    }

    /**
     * Called after starting of database
     */
    protected void afterDatabaseStart( DataGeneratorConfig config )
    {
    }

    /**
     * Used to define the size & structure of the store that will be used by this benchmark.
     *
     * @return the instance of {@link DataGeneratorConfig} that will guide store generation.
     */
    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .build();
    }

    protected void benchmarkTearDown() throws Exception
    {
    }

    // -----------------------------------------------------------------------------------------------------------------
    // --------------------------------------- Convenience Assert Result Methods ---------------------------------------
    // -----------------------------------------------------------------------------------------------------------------

    protected long assertCursorNotNull( NodeIndexCursor cursor )
    {
        if ( !cursor.next() )
        {
            throw new RuntimeException( "Node was null" );
        }
        return cursor.nodeReference();
    }

    protected void assertCount( NodeIndexCursor cursor, long expectedCount, Blackhole bh )
    {
        assertCount( cursor, expectedCount, expectedCount, bh );
    }

    protected void assertCount( NodeIndexCursor cursor, long minCount, long maxCount, Blackhole bh )
    {
        long count = 0;
        while ( cursor.next() )
        {
            bh.consume( cursor.nodeReference() );
            count++;
        }
        if ( count < minCount || count > maxCount )
        {
            throw new RuntimeException( "Expected " + minCount + " <= count <= " + maxCount + ") but found " + count );
        }
    }

    protected void assertCountAndValues( NodeValueIndexCursor cursor, long expectedCount, Blackhole bh )
    {
        long count = 0;
        while ( cursor.next() )
        {
            bh.consume( cursor.propertyValue( 0 ) );
            count++;
        }
        if ( count != expectedCount )
        {
            throw new RuntimeException( "Expected " + expectedCount + " values but found " + count );
        }
    }

    protected void assertCount( RelationshipScanCursor cursor, long expectedCount, Blackhole bh )
    {
        assertCount( cursor, expectedCount, expectedCount, bh );
    }

    protected void assertCount( RelationshipScanCursor cursor, long minCount, long maxCount, Blackhole bh )
    {
        long count = 0;
        while ( cursor.next() )
        {
            bh.consume( cursor.relationshipReference() );
            count++;
        }
        if ( count < minCount || count > maxCount )
        {
            throw new RuntimeException( "Expected " + minCount + " <= count <= " + maxCount + ") but found " + count );
        }
    }

    protected void assertCount( NodeCursor cursor, long expectedCount, Blackhole bh )
    {
        assertCount( cursor, expectedCount, expectedCount, bh );
    }

    protected void assertCount( NodeCursor cursor, long minCount, long maxCount, Blackhole bh )
    {
        long count = 0;
        while ( cursor.next() )
        {
            bh.consume( cursor.nodeReference() );
            count++;
        }
        if ( count < minCount || count > maxCount )
        {
            throw new RuntimeException( "Expected " + minCount + " <= count <= " + maxCount + ") but found " + count );
        }
    }

    protected void assertNotNull( Object entity, Blackhole bh )
    {
        if ( null == entity )
        {
            throw new RuntimeException( "Entity was null" );
        }
        bh.consume( entity );
    }

    protected void assertCount(
            Iterable<?> entities,
            long expectedCount,
            Blackhole bh )
    {
        consumeCountAssert( entities.iterator(), expectedCount, expectedCount, bh );
    }

    protected void assertCount(
            Iterator<?> entities,
            long expectedCount,
            Blackhole bh )
    {
        consumeCountAssert( entities, expectedCount, expectedCount, bh );
    }

    protected void assertCount(
            Iterator<?> entities,
            long min,
            long max,
            Blackhole bh )
    {
        consumeCountAssert( entities, min, max, bh );
    }

    private void consumeCountAssert(
            Iterator<?> entities,
            long min,
            long max,
            Blackhole bh )
    {
        long count = 0;
        while ( entities.hasNext() )
        {
            bh.consume( entities.next() );
            count++;
        }
        if ( count < min || count > max )
        {
            throw new RuntimeException( "Expected " + min + " <= count <= " + max + ") but found " + count );
        }
    }
}
