/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.benchmarks.Throttler;
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket;
import com.neo4j.bench.micro.data.IndexType;
import com.neo4j.bench.micro.data.LabelKeyDefinition;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorFactory;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.SplittableRandom;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.micro.data.DiscreteGenerator.discrete;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.nonContendingStridingFor;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class ConcurrentReadWriteNonUniqueNodePropertiesV2 extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 1_000_000;
    private static final Label LABEL = Label.label( "Label" );

    private static final String GROUP_NAME = "readWrite";
    // ratio reading (find node) threads to writing (remove/add property) threads: (write) 4:1 (read)
    private static final int WRITE_THREAD_RATIO = 4;
    private static final int HIGH_SELECTIVITY_READ_THREAD_RATIO = 1;
    private static final int MEDIUM_SELECTIVITY_READ_THREAD_RATIO = 1;
    private static final int LOW_SELECTIVITY_READ_THREAD_RATIO = 1;

    private static final int TARGET_WRITE_THROUGHPUT = 1000; // ops/second/thread

    private static final double HIGH_SELECTIVITY_RATIO = 1;
    private static final double MEDIUM_SELECTIVITY_RATIO = 10;
    private static final double LOW_SELECTIVITY_RATIO = 100;

    // number of nodes found by GraphDatabaseService::findNodes should be within 5% of expected value
    private static final double RESULT_COUNT_ACCURACY_TOLERANCE = 0.05;

    private Object highSelectivityValue;
    private Object mediumSelectivityValue;
    private Object lowSelectivityValue;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"NONE", "SCHEMA"},
            base = {"SCHEMA"} )
    @Param( {} )
    public IndexType index;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"100"} )
    @Param( {} )
    public int txSize;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @Override
    public String description()
    {
        return "Tests performance of retrieving nodes by label and property, in the present of updates.\n" +
               "Runs in three indexing scenarios: no index, schema index.\n" +
               "Method:\n" +
               "- Every node has exactly one label, same label\n" +
               "- Every node has exactly one property, same property (key)\n" +
               "- During store creation, property values are assigned with skewed policy\n" +
               "- There are three property values, with frequency of 1:10:100\n" +
               "- There is one read benchmark for each frequency:\n" +
               "    (1) Find Nodes with High selectivity property value: 1\n" +
               "    (2) Find Nodes with Medium selectivity property value: 10\n" +
               "    (3) Find Nodes with High selectivity property value: 100\n" +
               "- There is one write benchmark, which calls Node::setProperty on random nodes\n" +
               "- Values assigned to properties follow the same skewed policy as during initial store generation\n" +
               "- Ratio of read threads to write threads is (read) " +
               (HIGH_SELECTIVITY_READ_THREAD_RATIO +
                MEDIUM_SELECTIVITY_READ_THREAD_RATIO +
                LOW_SELECTIVITY_READ_THREAD_RATIO) + ":" + WRITE_THREAD_RATIO + " (write).\n" +
               "  * 1 read thread for each read benchmark\n" +
               "  * 4 write threads";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        Bucket[] buckets = getBuckets();
        highSelectivityValue = buckets[0].value();
        mediumSelectivityValue = buckets[1].value();
        lowSelectivityValue = buckets[2].value();
        PropertyDefinition propertyDefinition = getPropertyDefinition( buckets );
        DataGeneratorConfigBuilder builder = new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withLabels( LABEL )
                .withNodeProperties( propertyDefinition )
                .withNeo4jConfig( Neo4jConfigBuilder.empty().withSetting( record_format, format ).build() )
                .isReusableStore( false );
        switch ( index )
        {
        case SCHEMA:
            return builder.withSchemaIndexes( new LabelKeyDefinition( LABEL, propertyDefinition.key() ) ).build();
        case NONE:
            return builder.build();
        default:
            throw new RuntimeException( "Unsupported 'index': " + index );
        }
    }

    private PropertyDefinition getPropertyDefinition( Bucket[] buckets )
    {
        ValueGeneratorFactory values = discrete( buckets );
        return new PropertyDefinition( type, values );
    }

    private Bucket[] getBuckets()
    {
        double[] discreteBucketRatios = new double[]{
                HIGH_SELECTIVITY_RATIO,
                MEDIUM_SELECTIVITY_RATIO,
                LOW_SELECTIVITY_RATIO};
        return discreteBucketsFor( type, discreteBucketRatios );
    }

    private int expectedHighSelectivityCount()
    {
        return (int) ((NODE_COUNT * HIGH_SELECTIVITY_RATIO) /
                      (HIGH_SELECTIVITY_RATIO + MEDIUM_SELECTIVITY_RATIO + LOW_SELECTIVITY_RATIO));
    }

    private int expectedMediumSelectivityCount()
    {
        return (int) ((NODE_COUNT * MEDIUM_SELECTIVITY_RATIO) /
                      (HIGH_SELECTIVITY_RATIO + MEDIUM_SELECTIVITY_RATIO + LOW_SELECTIVITY_RATIO));
    }

    private int expectedLowSelectivityCount()
    {
        return (int) ((NODE_COUNT * LOW_SELECTIVITY_RATIO) /
                      (HIGH_SELECTIVITY_RATIO + MEDIUM_SELECTIVITY_RATIO + LOW_SELECTIVITY_RATIO));
    }

    private int minEstimateFor( int expectedCount )
    {
        return expectedCount - (int) (expectedCount * RESULT_COUNT_ACCURACY_TOLERANCE);
    }

    private int maxEstimateFor( int expectedCount )
    {
        return expectedCount + (int) (expectedCount * RESULT_COUNT_ACCURACY_TOLERANCE);
    }

    @State( Scope.Thread )
    public static class WriteTxState
    {
        Throttler throttler;
        TxBatch txBatch;
        ValueGeneratorFun<Long> ids;
        String key;
        ValueGeneratorFun values;

        @Setup
        public void setUp( ThreadParams threadParams, ConcurrentReadWriteNonUniqueNodePropertiesV2 benchmarkState )
        {
            throttler = new Throttler( TARGET_WRITE_THROUGHPUT );
            int threads = threadCountForSubgroupInstancesOf( threadParams );
            int thread = uniqueSubgroupThreadIdFor( threadParams );
            ids = nonContendingStridingFor(
                    LNG,
                    threads,
                    thread,
                    NODE_COUNT ).create();
            PropertyDefinition propertyDefinition = benchmarkState.getPropertyDefinition( benchmarkState.getBuckets() );
            key = propertyDefinition.key();
            values = propertyDefinition.value().create();
            txBatch = new TxBatch(
                    benchmarkState.db(),
                    benchmarkState.txSize );
        }

        long nodeId( SplittableRandom rng )
        {
            return ids.next( rng );
        }

        String key()
        {
            return key;
        }

        Object value( SplittableRandom rng )
        {
            return values.next( rng );
        }

        void advance()
        {
            txBatch.advance();
        }

        @TearDown
        public void tearDown()
        {
            txBatch.close();
        }
    }

    @State( Scope.Thread )
    public static class ReadTxState
    {
        Transaction tx;
        String propertyKey;
        int highSelectivityMin;
        int highSelectivityMax;
        int mediumSelectivityMin;
        int mediumSelectivityMax;
        int lowSelectivityMin;
        int lowSelectivityMax;

        @Setup
        public void setUp( ConcurrentReadWriteNonUniqueNodePropertiesV2 benchmarkState )
        {
            tx = benchmarkState.db().beginTx();
            PropertyDefinition propertyDefinition = benchmarkState.getPropertyDefinition( benchmarkState.getBuckets() );
            propertyKey = propertyDefinition.key();
            highSelectivityMin = benchmarkState.minEstimateFor( benchmarkState.expectedHighSelectivityCount() );
            highSelectivityMax = benchmarkState.maxEstimateFor( benchmarkState.expectedHighSelectivityCount() );
            mediumSelectivityMin = benchmarkState.minEstimateFor( benchmarkState.expectedMediumSelectivityCount() );
            mediumSelectivityMax = benchmarkState.maxEstimateFor( benchmarkState.expectedMediumSelectivityCount() );
            lowSelectivityMin = benchmarkState.minEstimateFor( benchmarkState.expectedLowSelectivityCount() );
            lowSelectivityMax = benchmarkState.maxEstimateFor( benchmarkState.expectedLowSelectivityCount() );
        }

        @TearDown
        public void tearDown()
        {
            tx.close();
        }
    }

    /**
     * Note: Mode.SampleTime purposely not used in combination with transaction batching.
     * <p>
     * Reason: invocations containing a transaction commit will have very different latency profile, resulting in
     * deceptively low percentile values for invocations that do not commit, and vice versa for invocations that do.
     * Making sense of those plots will be difficult.
     */
    @Benchmark
    @Group( GROUP_NAME )
    @GroupThreads( WRITE_THREAD_RATIO )
    @BenchmarkMode( {Mode.Throughput} )
    public void createDeleteLabel( WriteTxState writeTxState, RNGState rngState )
    {
        writeTxState.throttler.waitForNext();
        writeTxState.advance();
        Node node = writeTxState.txBatch.transaction().getNodeById( writeTxState.nodeId( rngState.rng ) );
        node.setProperty( writeTxState.key(), writeTxState.value( rngState.rng ) );
    }

    /**
     * Note: Mode.SampleTime not used because JMH runs benchmark once for every BenchmarkMode in the Group.
     * Throughput is used for the writing threads, with good reason.
     * To save on build time we do not want this benchmark to run twice.
     */
    @Benchmark
    @Group( GROUP_NAME )
    @GroupThreads( HIGH_SELECTIVITY_READ_THREAD_RATIO )
    @BenchmarkMode( {Mode.Throughput} )
    public void countNodesWithLabelKeyValueWhenSelectivityHigh( ReadTxState txState, Blackhole bh )
    {
        assertCount(
                txState.tx.findNodes( LABEL, txState.propertyKey, highSelectivityValue ),
                txState.highSelectivityMin,
                txState.highSelectivityMax,
                bh );
    }

    @Benchmark
    @Group( GROUP_NAME )
    @GroupThreads( MEDIUM_SELECTIVITY_READ_THREAD_RATIO )
    @BenchmarkMode( {Mode.Throughput} )
    public void countNodesWithLabelKeyValueWhenSelectivityMedium( ReadTxState txState, Blackhole bh )
    {
        assertCount(
                txState.tx.findNodes( LABEL, txState.propertyKey, mediumSelectivityValue ),
                txState.mediumSelectivityMin,
                txState.mediumSelectivityMax,
                bh );
    }

    @Benchmark
    @Group( GROUP_NAME )
    @GroupThreads( LOW_SELECTIVITY_READ_THREAD_RATIO )
    @BenchmarkMode( {Mode.Throughput} )
    public void countNodesWithLabelKeyValueWhenSelectivityLow( ReadTxState txState, Blackhole bh )
    {
        assertCount(
                txState.tx.findNodes( LABEL, txState.propertyKey, lowSelectivityValue ),
                txState.lowSelectivityMin,
                txState.lowSelectivityMax,
                bh );
    }
}
