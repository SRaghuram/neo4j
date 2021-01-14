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
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.data.IndexType;
import com.neo4j.bench.data.LabelKeyDefinition;
import com.neo4j.bench.data.ValueGeneratorFun;
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

import org.neo4j.graphdb.Label;

import static com.neo4j.bench.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.nonContendingStridingFor;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateUniqueNodeProperties extends AbstractCoreBenchmark
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
            base = {LNG, STR_SML, DATE_TIME, POINT} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"NONE", "SCHEMA", "UNIQUE"},
            base = {"NONE", "SCHEMA"} )
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
        return "Tests performance of unique node property creation, using different transaction batch sizes.\n" +
               "Creates unique key:value property pairs (allows comparison between index and unique constraints).\n" +
               "Runs three indexing scenarios: no index, schema index, unique constraint.\n" +
               "Guarantees unique values in the presence of parallelism:\n" +
               "- Every node has exactly one, same label\n" +
               "- Threads work on node ID sequences\n" +
               "- Sequence of every thread is guaranteed to never overlap with that of another thread\n" +
               "- Every thread starts at different offset (to accelerate warmup) in range, then wraps at max\n" +
               "- When a sequence wraps the thread moves onto the next property key\n" +
               "- Guarantees that for any property, each node ID appears in the sequence of exactly one thread\n" +
               "- As this guarantees property(key):node(id) uniqueness, same policy is used for property values\n" +
               "- I.e., value assigned to node property is ID of that node (in appropriate type)\n" +
               "Outcome:\n" +
               "- No two threads will ever create a property on the same node (avoids deadlocks)\n" +
               "- Every node will have the same properties\n" +
               "- No two nodes will have the same value for the same property";
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
                .withNeo4jConfig( Neo4jConfigBuilder.empty().withSetting( record_format, format ).build() )
                .isReusableStore( false )
                .build();
    }

    LabelKeyDefinition[] schemaIndexes( String[] keys )
    {
        return (IndexType.SCHEMA.equals( index ))
               ? Stream.of( keys )
                       .map( key -> new LabelKeyDefinition( LABEL, key ) )
                       .toArray( LabelKeyDefinition[]::new )
               : new LabelKeyDefinition[0];
    }

    LabelKeyDefinition[] uniquenessConstraints( String[] keys )
    {
        return (IndexType.UNIQUE.equals( index ))
               ? Stream.of( keys )
                       .map( key -> new LabelKeyDefinition( LABEL, key ) )
                       .toArray( LabelKeyDefinition[]::new )
               : new LabelKeyDefinition[0];
    }

    @State( Scope.Thread )
    public static class TxState
    {
        TxBatch txBatch;
        ValueGeneratorFun<Long> ids;
        int keyId;
        ValueGeneratorFun<?> values;
        long nodeId;

        @Setup
        public void setUp( ThreadParams threadParams, CreateUniqueNodeProperties benchmarkState )
        {
            ids = nonContendingStridingFor(
                    LNG,
                    threadParams.getThreadCount(),
                    threadParams.getThreadIndex(),
                    NODE_COUNT ).create();
            values = nonContendingStridingFor(
                    benchmarkState.type,
                    threadParams.getThreadCount(),
                    threadParams.getThreadIndex(),
                    NODE_COUNT ).create();
            keyId = 0;
            nodeId = -1;
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.txSize );
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
        txState.txBatch.transaction().getNodeById( txState.nodeId() ).setProperty( txState.key(), txState.value( rngState.rng ) );
    }
}
