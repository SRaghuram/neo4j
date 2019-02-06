/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.LabelKeyDefinition;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.benchmarks.core.FindNodeNonUnique.LABEL;
import static com.neo4j.bench.micro.benchmarks.core.FindNodeNonUnique.NODE_COUNT;
import static com.neo4j.bench.micro.benchmarks.core.FindNodeNonUnique.expectedHighSelectivityCount;
import static com.neo4j.bench.micro.benchmarks.core.FindNodeNonUnique.expectedLowSelectivityCount;
import static com.neo4j.bench.micro.benchmarks.core.FindNodeNonUnique.expectedMediumSelectivityCount;
import static com.neo4j.bench.micro.benchmarks.core.FindNodeNonUnique.getBuckets;
import static com.neo4j.bench.micro.benchmarks.core.FindNodeNonUnique.getPropertyDefinition;
import static com.neo4j.bench.micro.benchmarks.core.FindNodeNonUnique.maxEstimateFor;
import static com.neo4j.bench.micro.benchmarks.core.FindNodeNonUnique.minEstimateFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.TIME;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class FindNodeNonUnique extends AbstractKernelBenchmark
{
    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation FindNodeNonUnique_kernelImplementation;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String FindNodeNonUnique_format;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT},
            base = {LNG, STR_SML, DATE_TIME, POINT} )
    @Param( {} )
    public String FindNodeNonUnique_type;

    private Value highSelectivityValue;
    private Value mediumSelectivityValue;
    private Value lowSelectivityValue;

    @Override
    public String description()
    {
        return "Tests performance of retrieving nodes by label and property using index seek.\n" +
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
        Bucket[] buckets = getBuckets( FindNodeNonUnique_type );
        highSelectivityValue = Values.of( buckets[0].value() );
        mediumSelectivityValue = Values.of( buckets[1].value() );
        lowSelectivityValue = Values.of( buckets[2].value() );
        PropertyDefinition propertyDefinition = getPropertyDefinition( buckets, FindNodeNonUnique_type );
        DataGeneratorConfigBuilder builder = new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withLabels( LABEL )
                .withNodeProperties( propertyDefinition )
                .isReusableStore( true );
        return builder.withSchemaIndexes( new LabelKeyDefinition( LABEL, propertyDefinition.key() ) ).build();
    }

    private String getPropertyKey()
    {
        Bucket[] buckets = getBuckets( FindNodeNonUnique_type );
        return getPropertyDefinition( buckets, FindNodeNonUnique_type ).key();
    }

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return FindNodeNonUnique_kernelImplementation;
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        int labelId;
        int propertyKey;

        int highSelectivityMin;
        int highSelectivityMax;
        int mediumSelectivityMin;
        int mediumSelectivityMax;
        int lowSelectivityMin;
        int lowSelectivityMax;

        CapableIndexReference index;
        NodeValueIndexCursor node;
        Read read;

        @Setup
        public void setUp( FindNodeNonUnique benchmark ) throws Exception
        {
            initializeTx( benchmark );
            propertyKey = kernelTx.token.propertyKey( benchmark.getPropertyKey() );
            labelId = labelToId( LABEL );

            highSelectivityMin = minEstimateFor( expectedHighSelectivityCount() );
            highSelectivityMax = maxEstimateFor( expectedHighSelectivityCount() );
            mediumSelectivityMin = minEstimateFor( expectedMediumSelectivityCount() );
            mediumSelectivityMax = maxEstimateFor( expectedMediumSelectivityCount() );
            lowSelectivityMin = minEstimateFor( expectedLowSelectivityCount() );
            lowSelectivityMax = maxEstimateFor( expectedLowSelectivityCount() );

            index = kernelTx.schemaRead.index( labelId, propertyKey );
            node = kernelTx.cursors.allocateNodeValueIndexCursor();
            read = kernelTx.read;
        }

        @TearDown
        public void tearDown() throws Exception
        {
            closeTx();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValueWhenSelectivityHigh( TxState txState, Blackhole bh ) throws KernelException
    {
        IndexQuery query = IndexQuery.exact( txState.propertyKey, highSelectivityValue );
        txState.read.nodeIndexSeek( txState.index, txState.node, IndexOrder.NONE, query );
        assertCount( txState.node, txState.highSelectivityMin, txState.highSelectivityMax, bh );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValueWhenSelectivityMedium( TxState txState, Blackhole bh ) throws KernelException
    {
        IndexQuery query = IndexQuery.exact( txState.propertyKey, mediumSelectivityValue );
        txState.read.nodeIndexSeek( txState.index, txState.node, IndexOrder.NONE, query );
        assertCount( txState.node, txState.mediumSelectivityMin, txState.mediumSelectivityMax, bh );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValueWhenSelectivityLow( TxState txState, Blackhole bh ) throws KernelException
    {
        IndexQuery query = IndexQuery.exact( txState.propertyKey, lowSelectivityValue );
        txState.read.nodeIndexSeek( txState.index, txState.node, IndexOrder.NONE, query );
        assertCount( txState.node, txState.lowSelectivityMin, txState.lowSelectivityMax, bh );
    }

    public static void main( String... methods ) throws Exception
    {
        run( FindNodeNonUnique.class, methods );
    }
}
