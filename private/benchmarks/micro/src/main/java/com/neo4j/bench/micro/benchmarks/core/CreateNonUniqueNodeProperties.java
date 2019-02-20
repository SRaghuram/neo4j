/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.SplittableRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.IndexType;
import com.neo4j.bench.micro.data.LabelKeyDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorFactory;
import com.neo4j.bench.micro.data.ValueGeneratorFun;

import org.neo4j.graphdb.Label;

import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.nonContendingStridingFor;

@BenchmarkEnabled( true )
public class CreateNonUniqueNodeProperties extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 10_000_000;
    private static final Label LABEL = Label.label( "Label" );
    // Controls how many indexes are created
    private static final int MAX_PROPERTIES_PER_NODE = 10;
    private static final String[] KEYS = IntStream.range( 0, MAX_PROPERTIES_PER_NODE ).boxed()
            .map( i -> "key" + i )
            .toArray( String[]::new );

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String CreateNonUniqueNodeProperties_type;

    @ParamValues(
            allowed = {"NONE", "SCHEMA"},
            base = {"SCHEMA"} )
    @Param( {} )
    public IndexType CreateNonUniqueNodeProperties_index;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"100"} )
    @Param( {} )
    public int CreateNonUniqueNodeProperties_txSize;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String CreateNonUniqueNodeProperties_format;

    @ParamValues(
            allowed = {"off_heap", "on_heap"},
            base = {"on_heap"} )
    @Param( {} )
    public String CreateNonUniqueNodeProperties_txMemory;

    @Override
    public String description()
    {
        return "Tests performance of non-unique node property creation, with different transaction batch sizes.\n" +
                "Benchmark creates key:value node property pairs.\n" +
                "Runs in two indexing scenarios: no index, schema index.\n" +
                "Setup is as follows:\n" +
                "- Every node has exactly one, same label\n" +
                "- Threads work on node ID sequences\n" +
                "- Sequence of every thread is guaranteed to never overlap with that of another thread\n" +
                "- Every thread starts at different offset (to accelerate warmup) in range, then wraps at max\n" +
                "- When a sequence wraps the thread moves onto the next property key\n" +
                "- Guarantees that for any property, each node ID appears in the sequence of exactly one thread\n" +
                "- For property value generation a skewed distribution is used\n" +
                "- There are three property values, with frequency of 1:10:100\n" +
                "Outcome:\n" +
                "- No two threads will ever create a property on the same node (avoids deadlocks)\n" +
                "- Every node will have the same properties\n" +
                "- Multiple nodes will have the same value for the same property";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withLabels( LABEL )
                .withSchemaIndexes( schemaIndexes( KEYS ) )
                .withUniqueConstraints( uniquenessConstraints( KEYS ) )
                .isReusableStore( false )
                .withNeo4jConfig( Neo4jConfig.empty().setTransactionMemory( CreateNonUniqueNodeProperties_txMemory ) )
                .build();
    }

    private LabelKeyDefinition[] schemaIndexes( String[] keys )
    {
        return (IndexType.SCHEMA.equals( CreateNonUniqueNodeProperties_index ))
               ? Stream.of( keys )
                       .map( key -> new LabelKeyDefinition( LABEL, key ) )
                       .toArray( LabelKeyDefinition[]::new )
               : new LabelKeyDefinition[0];
    }

    private LabelKeyDefinition[] uniquenessConstraints( String[] keys )
    {
        return (IndexType.UNIQUE.equals( CreateNonUniqueNodeProperties_index ))
               ? Stream.of( keys )
                       .map( key -> new LabelKeyDefinition( LABEL, key ) )
                       .toArray( LabelKeyDefinition[]::new )
               : new LabelKeyDefinition[0];
    }

    private ValueGeneratorFactory getValueGeneratorFactory()
    {
        double[] discreteBucketRatios = new double[]{1, 10, 100};
        return discreteFor( CreateNonUniqueNodeProperties_type, discreteBucketRatios );
    }

    @State( Scope.Thread )
    public static class TxState
    {
        TxBatch txBatch;
        ValueGeneratorFun<Long> ids;
        int keyId;
        ValueGeneratorFun values;
        long nodeId;

        @Setup
        public void setUp( ThreadParams threadParams, CreateNonUniqueNodeProperties benchmarkState )
        {
            ids = nonContendingStridingFor(
                    LNG,
                    threadParams.getThreadCount(),
                    threadParams.getThreadIndex(),
                    NODE_COUNT ).create();
            values = benchmarkState.getValueGeneratorFactory().create();
            keyId = 0;
            nodeId = -1;
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.CreateNonUniqueNodeProperties_txSize );
        }

        long nodeId()
        {
            return nodeId;
        }

        String key()
        {
            return KEYS[keyId];
        }

        Object value( SplittableRandom rng )
        {
            return values.next( rng );
        }

        void advance( SplittableRandom rng )
        {
            txBatch.advance();
            nodeId = ids.next( rng );
            if ( ids.wrapped() )
            {
                keyId++;
            }
        }

        @TearDown
        public void tearDown()
        {
            txBatch.close();
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
    @BenchmarkMode( {Mode.Throughput} )
    public void setProperty( TxState txState, RNGState rngState )
    {
        txState.advance( rngState.rng );
        db().getNodeById( txState.nodeId() ).setProperty( txState.key(), txState.value( rngState.rng ) );
    }
}
