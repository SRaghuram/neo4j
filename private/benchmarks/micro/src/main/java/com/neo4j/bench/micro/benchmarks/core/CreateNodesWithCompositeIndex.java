/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.LabelKeyDefinition;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateNodesWithCompositeIndex extends AbstractCoreBenchmark
{
    private static final Label LABEL = Label.label( "Label" );

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"1"} )
    @Param( {} )
    public int CreateNodesWithCompositeIndex_txSize;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String CreateNodesWithCompositeIndex_format;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String CreateNodesWithCompositeIndex_type;

    @ParamValues(
            allowed = {"2", "4", "8"},
            base = {"2", "8"} )
    @Param( {} )
    public int CreateNodesWithCompositeIndex_keys;

    @Override
    public String description()
    {
        return "Tests performance of creating nodes with label and properties, with/without composite index";
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
                .withSchemaIndexes( new LabelKeyDefinition( LABEL, keys() ) )
                .withNeo4jConfig( Neo4jConfig.empty().withSetting( record_format, CreateNodesWithCompositeIndex_format ) )
                .isReusableStore( false )
                .build();
    }

    private String[] keys()
    {
        return Arrays.stream( propertyDefinitions() ).map( PropertyDefinition::key ).toArray( String[]::new );
    }

    private PropertyDefinition[] propertyDefinitions()
    {
        return IntStream.range( 0, CreateNodesWithCompositeIndex_keys )
                        .mapToObj( i -> new PropertyDefinition(
                                CreateNodesWithCompositeIndex_type + "_" + i,
                                randPropertyFor( CreateNodesWithCompositeIndex_type ).value() ) )
                        .toArray( PropertyDefinition[]::new );
    }

    @State( Scope.Thread )
    public static class TxState
    {
        TxBatch txBatch;
        ValueGeneratorFun values;
        String[] keys;

        @Setup
        public void setUp( CreateNodesWithCompositeIndex benchmarkState )
                throws InterruptedException
        {
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.CreateNodesWithCompositeIndex_txSize );
            keys = benchmarkState.keys();
            values = randPropertyFor( benchmarkState.CreateNodesWithCompositeIndex_type ).value().create();
        }

        void advance()
        {
            txBatch.advance();
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
    public void createNode( TxState txState, RNGState rngState )
    {
        txState.advance();
        Node node = db().createNode( LABEL );
        for ( int i = 0; i < txState.keys.length; i++ )
        {
            node.setProperty( txState.keys[i], txState.nextValue( rngState.rng ) );
        }
    }
}
