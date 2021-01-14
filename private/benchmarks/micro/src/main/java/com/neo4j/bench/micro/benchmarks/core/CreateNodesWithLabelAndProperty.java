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

import java.util.SplittableRandom;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

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
import static com.neo4j.bench.data.ValueGeneratorUtil.randPropertyFor;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateNodesWithLabelAndProperty extends AbstractCoreBenchmark
{
    private static final Label LABEL = Label.label( "Label" );
    private static final String KEY = "key";

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"1"} )
    @Param( {} )
    public int txSize;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"NONE", "SCHEMA"},
            base = {"NONE", "SCHEMA"} )
    @Param( {} )
    public IndexType index;

    @ParamValues(
            allowed = {"off_heap", "on_heap", "default"},
            base = {"default"} )
    @Param( {} )
    public String txMemory;

    @Override
    public String description()
    {
        return "Tests performance of creating nodes with label and property";
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
                .withSchemaIndexes( schemaIndexes() )
                .withNeo4jConfig( Neo4jConfigBuilder
                                          .empty()
                                          .withSetting( record_format, format )
                                          .setTransactionMemory( txMemory )
                                          .build() )
                .isReusableStore( false )
                .build();
    }

    private LabelKeyDefinition[] schemaIndexes()
    {
        return (IndexType.SCHEMA.equals( index ))
               ? new LabelKeyDefinition[]{new LabelKeyDefinition( LABEL, KEY )}
               : new LabelKeyDefinition[0];
    }

    @State( Scope.Thread )
    public static class TxState
    {
        TxBatch txBatch;
        ValueGeneratorFun values;

        @Setup
        public void setUp( CreateNodesWithLabelAndProperty benchmarkState ) throws InterruptedException
        {
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.txSize );
            values = randPropertyFor( benchmarkState.type ).value().create();
        }

        void advance()
        {
            txBatch.advance();
        }

        Transaction transaction()
        {
            return txBatch.transaction();
        }

        Object nextValue( SplittableRandom rng )
        {
            return values.next( rng );
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
    public void createNodeWithLabelAndProperty( TxState txState, RNGState rngState )
    {
        txState.advance();
        txState.transaction().createNode( LABEL ).setProperty( KEY, txState.nextValue( rngState.rng ) );
    }
}
