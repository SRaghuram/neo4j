/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.data.DiscreteGenerator.Bucket;
import com.neo4j.bench.data.IndexType;
import com.neo4j.bench.data.LabelKeyDefinition;
import com.neo4j.bench.data.PropertyDefinition;
import com.neo4j.bench.data.ValueGeneratorFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.data.DiscreteGenerator.discrete;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.discreteBucketsFor;

@BenchmarkEnabled( true )
public class FindNodeNonUnique extends AbstractCoreBenchmark
{
    public static final Label LABEL = Label.label( "Label" );
    public static final int NODE_COUNT = 1_000_000;
    private static final double HIGH_SELECTIVITY_RATIO = 1;
    private static final double MEDIUM_SELECTIVITY_RATIO = 10;
    private static final double LOW_SELECTIVITY_RATIO = 100;
    private static final double TOLERATED_CARDINALITY_ESTIMATE_ERROR = 0.05;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"NONE", "SCHEMA"},
            base = {"SCHEMA"} )
    @Param( {} )
    public IndexType index;

    @ParamValues(
            allowed = {"off_heap", "on_heap", "default"},
            base = {"default"} )
    @Param( {} )
    public String txMemory;

    private Object highSelectivityValue;
    private Object mediumSelectivityValue;
    private Object lowSelectivityValue;

    @Override
    public String description()
    {
        return "Tests performance of retrieving nodes by label and property.\n" +
               "Runs in two indexing scenarios: no index, schema index.\n" +
               "Method:\n" +
               "- Every node has exactly one label, same label\n" +
               "- Every node has exactly one property, same property (key)\n" +
               "- During store creation, property values are assigned with skewed policy\n" +
               "- There are three property values, with frequency of 1:10:100\n" +
               "- When reading, there is one benchmark for each frequency:\n" +
               "    * High Selectivity: 1\n" +
               "    * Medium Selectivity: 10\n" +
               "    * Low Selectivity: 100";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        Bucket[] buckets = getBuckets( type );
        highSelectivityValue = buckets[0].value();
        mediumSelectivityValue = buckets[1].value();
        lowSelectivityValue = buckets[2].value();
        PropertyDefinition propertyDefinition = getPropertyDefinition( buckets, type );
        DataGeneratorConfigBuilder builder = new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withLabels( LABEL )
                .withNodeProperties( propertyDefinition )
                .withNeo4jConfig( Neo4jConfigBuilder.empty().setTransactionMemory( txMemory ).build() )
                .isReusableStore( true );
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

    private PropertyDefinition getPropertyDefinition()
    {
        Bucket[] buckets = getBuckets( type );
        return getPropertyDefinition( buckets, type );
    }

    public static PropertyDefinition getPropertyDefinition( Bucket[] buckets, String type )
    {
        ValueGeneratorFactory values = discrete( buckets );
        return new PropertyDefinition( type, values );
    }

    public static Bucket[] getBuckets( String type )
    {
        double[] discreteBucketRatios = new double[]{
                HIGH_SELECTIVITY_RATIO,
                MEDIUM_SELECTIVITY_RATIO,
                LOW_SELECTIVITY_RATIO};
        return discreteBucketsFor( type, discreteBucketRatios );
    }

    public static int expectedHighSelectivityCount()
    {
        return (int) ((NODE_COUNT * HIGH_SELECTIVITY_RATIO) /
                      (HIGH_SELECTIVITY_RATIO + MEDIUM_SELECTIVITY_RATIO + LOW_SELECTIVITY_RATIO));
    }

    public static int expectedMediumSelectivityCount()
    {
        return (int) ((NODE_COUNT * MEDIUM_SELECTIVITY_RATIO) /
                      (HIGH_SELECTIVITY_RATIO + MEDIUM_SELECTIVITY_RATIO + LOW_SELECTIVITY_RATIO));
    }

    public static int expectedLowSelectivityCount()
    {
        return (int) ((NODE_COUNT * LOW_SELECTIVITY_RATIO) /
                      (HIGH_SELECTIVITY_RATIO + MEDIUM_SELECTIVITY_RATIO + LOW_SELECTIVITY_RATIO));
    }

    public static int minEstimateFor( int expectedCount )
    {
        return expectedCount - (int) (expectedCount * TOLERATED_CARDINALITY_ESTIMATE_ERROR);
    }

    public static int maxEstimateFor( int expectedCount )
    {
        return expectedCount + (int) (expectedCount * TOLERATED_CARDINALITY_ESTIMATE_ERROR);
    }

    @State( Scope.Thread )
    public static class TxState
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
        public void setUp( FindNodeNonUnique benchmarkState ) throws InterruptedException
        {
            tx = benchmarkState.db().beginTx();
            propertyKey = benchmarkState.getPropertyDefinition().key();

            highSelectivityMin = minEstimateFor( expectedHighSelectivityCount() );
            highSelectivityMax = maxEstimateFor( expectedHighSelectivityCount() );
            mediumSelectivityMin = minEstimateFor( expectedMediumSelectivityCount() );
            mediumSelectivityMax = maxEstimateFor( expectedMediumSelectivityCount() );
            lowSelectivityMin = minEstimateFor( expectedLowSelectivityCount() );
            lowSelectivityMax = maxEstimateFor( expectedLowSelectivityCount() );
        }

        @TearDown
        public void tearDown()
        {
            tx.close();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValueWhenSelectivityHigh( TxState txState, Blackhole bh )
    {
        assertCount(
                txState.tx.findNodes( LABEL, txState.propertyKey, highSelectivityValue ),
                txState.highSelectivityMin,
                txState.highSelectivityMax,
                bh );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValueWhenSelectivityMedium( TxState txState, Blackhole bh )
    {
        assertCount(
                txState.tx.findNodes( LABEL, txState.propertyKey, mediumSelectivityValue ),
                txState.mediumSelectivityMin,
                txState.mediumSelectivityMax,
                bh );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValueWhenSelectivityLow( TxState txState, Blackhole bh )
    {
        assertCount(
                txState.tx.findNodes( LABEL, txState.propertyKey, lowSelectivityValue ),
                txState.lowSelectivityMin,
                txState.lowSelectivityMax,
                bh );
    }
}
