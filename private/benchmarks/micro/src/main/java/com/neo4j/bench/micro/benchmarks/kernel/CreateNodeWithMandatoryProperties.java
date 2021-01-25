/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.LabelKeyDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
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

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.values.storable.Value;

import static com.neo4j.bench.micro.Main.run;
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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.stridingFor;

@BenchmarkEnabled( true )
public class CreateNodeWithMandatoryProperties extends AbstractKernelBenchmark
{
    private static final Label LABEL = Label.label( "Label" );
    private static final String KEY = "key";

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"100"} )
    @Param( {} )
    public int txSize;

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernel;

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return kernel;
    }

    @Override
    public String description()
    {
        return "Tests performance of creating nodes with mandatory constraints, " +
               "using different transaction batch sizes.\n" +
               "Method:\n" +
               "- Threads create nodes, batching multiple writes per transaction\n" +
               "- Every time a node is created it has one label, always the same label\n" +
               "- Every time a node is created it has one property, always with same key, but different value\n" +
               "- There is a mandatory constraint on the label:property pair being written";
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
                .withMandatoryNodeConstraints( new LabelKeyDefinition( LABEL, KEY ) )
                .isReusableStore( false )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        ValueGeneratorFun values;
        int key, label;

        @Setup
        public void setUp( ThreadParams threadParams, CreateNodeWithMandatoryProperties benchmarkState )
                throws KernelException
        {
            initializeTx( benchmarkState, benchmarkState.txSize );
            int stride = threadParams.getThreadCount();
            int offset = threadParams.getThreadIndex();
            // sequence should never wrap, 'sliding' value is irrelevant
            boolean sliding = false;
            values = stridingFor(
                    benchmarkState.type,
                    Integer.MAX_VALUE,
                    stride,
                    offset,
                    sliding ).create();
            label = kernelTx.token.labelGetOrCreateForName( LABEL.name() );
            key = kernelTx.token.propertyKeyGetOrCreateForName( KEY );
        }

        Value value( SplittableRandom rng )
        {
            return values.nextValue( rng );
        }

        @TearDown
        public void tearDown() throws Exception
        {
            closeTx();
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
    public Value createNode( TxState txState, RNGState rngState )
            throws KernelException
    {
        txState.kernelTx.advance();
        Write write = txState.kernelTx.write;
        long nodeId = write.nodeCreate();
        write.nodeAddLabel( nodeId, txState.label );
        return write.nodeSetProperty( nodeId, txState.key, txState.value( rngState.rng ) );
    }

    public static void main( String... methods ) throws Exception
    {
        run( CreateNodeWithMandatoryProperties.class, methods );
    }
}
