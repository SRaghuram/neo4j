package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
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

import org.neo4j.graphdb.Label;

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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.stridingFor;

@BenchmarkEnabled( true )
public class CreateNodeWithMandatoryProperties extends AbstractCoreBenchmark
{
    private static final Label LABEL = Label.label( "Label" );
    private static final String KEY = "key";

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String CreateNodeWithMandatoryProperties_type;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"100"} )
    @Param( {} )
    public int CreateNodeWithMandatoryProperties_txSize;

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
    public static class TxState
    {
        TxBatch txBatch;
        ValueGeneratorFun values;

        @Setup
        public void setUp( ThreadParams threadParams, CreateNodeWithMandatoryProperties benchmarkState )
        {
            int stride = threadParams.getThreadCount();
            int offset = threadParams.getThreadIndex();
            // sequence should never wrap, 'sliding' value is irrelevant
            boolean sliding = false;
            values = stridingFor(
                    benchmarkState.CreateNodeWithMandatoryProperties_type,
                    Integer.MAX_VALUE,
                    stride,
                    offset,
                    sliding ).create();
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.CreateNodeWithMandatoryProperties_txSize );
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
        db().createNode( LABEL ).setProperty( KEY, txState.value( rngState.rng ) );
    }
}
