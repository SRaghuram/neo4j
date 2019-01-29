package com.neo4j.bench.micro.benchmarks.marketing;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
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

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;

@BenchmarkEnabled( false )
public class CreateNodesWithProperty extends AbstractMarketingBenchmark
{
    private static final Label LABEL = DynamicLabel.label( "Label" );
    private static final Label SCHEMA_LABEL = DynamicLabel.label( "SchemaLabel" );
    private static final String SCHEMA_PROPERTY = "SchemaProperty";
    private static final PropertyDefinition[] propertyDefinitions = new PropertyDefinition[]
            {
                    randPropertyFor( INT, SCHEMA_PROPERTY ),
                    randPropertyFor( LNG ),
                    randPropertyFor( STR_SML )
            };

    @ParamValues(
            allowed = {"1", "100"},
            base = {"1", "100"} )
    @Param( {} )
    public int CreateNodesWithProperty_txSize;

    @ParamValues(
            allowed = {"standard"},
            base = {"standard"} )
    @Param( {} )
    public String CreateNodesWithProperty_format;

    @Override
    public String description()
    {
        return "Tests performance of node creation, using different transaction batch sizes.";
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
                .isReusableStore( false )
                .withSchemaIndexes( new LabelKeyDefinition( SCHEMA_LABEL, SCHEMA_PROPERTY ) )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        TxBatch txBatch;
        ValueGeneratorFun[] valueGeneratorFun;

        @Setup
        public void setUp( CreateNodesWithProperty benchmarkState ) throws InterruptedException
        {
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.CreateNodesWithProperty_txSize );
            valueGeneratorFun = Arrays.stream( propertyDefinitions )
                    .map( pd -> pd.value().create() )
                    .toArray( ValueGeneratorFun[]::new );
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

    private void withProperties( Node node, ValueGeneratorFun[] valueGeneratorFun, SplittableRandom rng )
    {
        for ( int i = 0; i < propertyDefinitions.length; i++ )
        {
            node.setProperty( propertyDefinitions[i].key(), valueGeneratorFun[i].next( rng ) );
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
        withProperties( db().createNode(), txState.valueGeneratorFun, rngState.rng );
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
    public void createNodeWithLabel( TxState txState, RNGState rngState )
    {
        txState.advance();
        withProperties( db().createNode( LABEL ), txState.valueGeneratorFun, rngState.rng );
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
    public void createNodeWithSchemaLabel( TxState txState, RNGState rngState )
    {
        txState.advance();
        withProperties( db().createNode( SCHEMA_LABEL ), txState.valueGeneratorFun, rngState.rng );
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
    public void createNodeWithLabelAndSchemaLabel( TxState txState, RNGState rngState )
    {
        txState.advance();
        withProperties( db().createNode( SCHEMA_LABEL, LABEL ), txState.valueGeneratorFun, rngState.rng );
    }
}
