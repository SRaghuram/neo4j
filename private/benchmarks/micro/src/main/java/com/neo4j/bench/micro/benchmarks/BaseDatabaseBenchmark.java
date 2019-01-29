package com.neo4j.bench.micro.benchmarks;

import com.neo4j.bench.micro.JMHResultUtil;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.data.Augmenterizer;
import com.neo4j.bench.micro.data.Augmenterizer.NullAugmenterizer;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.ManagedStore;
import com.neo4j.bench.micro.data.Stores;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Benchmark classes that extend this class will operate with the following life-cycle:
 * <br>
 * <br>
 * Setup:
 * <ol>
 * <li>Base store generation is performed, using the <code>DataGeneratorConfig</code> returned by <code>getConfig()</code></li>
 * <li>The <code>Augmenterizer</code> returned by <code>augmentDataGeneration()</code> is now invoked, to perform additional store generation</li>
 * <li>After store generation & before starting the database, <code>afterDataGeneration()</code> will be invoked.
 * Override this callback to mess with the generated store, or to prevent the database from starting.</li>
 * <li>After the database is started <code>afterDatabaseStart()</code> will be called.
 * Note that if <code>afterDataGeneration()</code> requests for the database to not be started this callback will never be called.</li>
 * <li>Actual benchmark starts</li>
 * </ol>
 * Tear Down:
 * <ol>
 * <li>After benchmark completes <code>databaseBenchmarkTearDown()</code> will be called, teardown logic should live in there</li>
 * </ol>
 */
@State( Scope.Benchmark )
public abstract class BaseDatabaseBenchmark implements Neo4jBenchmark
{
    @Param( {} )
    public String baseNeo4jConfig;

    @Param( {} )
    public String storesDir;

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

    protected Path store()
    {
        return managedStore.store();
    }

    @Setup
    public final void setUp( BenchmarkParams params ) throws Exception
    {
        Stores stores = new Stores( Paths.get( storesDir ) );
        Neo4jConfig neo4jConfig = Neo4jConfig.fromJson( baseNeo4jConfig );
        BenchmarkGroup group = JMHResultUtil.toBenchmarkGroup( params );
        Benchmark benchmark = JMHResultUtil.toBenchmarks( params ).parentBenchmark();

        Augmenterizer augmenterizer = augmentDataGeneration();
        managedStore = new ManagedStore( stores );
        managedStore.prepareDb( group, benchmark, getConfig(), neo4jConfig, augmenterizer );
        if ( afterDataGeneration().equals( StartDatabaseInstruction.START_DB ) )
        {
            managedStore.startDb();
            afterDatabaseStart();
        }
        else
        {
            System.out.println( "Database will not be started!" );
        }
    }

    @TearDown
    public final void tearDown() throws Exception
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
     * Return value specifies whether store should be started.
     */
    protected StartDatabaseInstruction afterDataGeneration()
    {
        return StartDatabaseInstruction.START_DB;
    }

    /**
     * Called after starting of database
     */
    protected void afterDatabaseStart()
    {
    }

    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder().build();
    }

    protected void benchmarkTearDown() throws Exception
    {

    }

    // -----------------------------------------------------------------------------------------------------------------
    // --------------------------------------- Convenience Assert Result Methods ---------------------------------------
    // -----------------------------------------------------------------------------------------------------------------

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
