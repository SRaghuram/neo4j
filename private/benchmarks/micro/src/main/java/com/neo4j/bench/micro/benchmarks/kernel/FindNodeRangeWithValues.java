/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.LabelKeyDefinition;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorUtil.Range;
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

import java.util.SplittableRandom;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.values.storable.Value;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.benchmarks.core.ReadAll.LABEL;
import static com.neo4j.bench.micro.benchmarks.core.ReadAll.NODE_COUNT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.ascGeneratorFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.ascPropertyFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.defaultRangeFor;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class FindNodeRangeWithValues extends AbstractKernelBenchmark
{
    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation FindNodeRangeWithValues_kernelImplementation;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String FindNodeRangeWithValues_format;

    /*
    NOTE: POINT is not enabled because, at present, nodes are filled using <ascending> policy and queried using <random> policy.
          For other generators this covers the same space. For POINT <random> has larger space than <ascending>, therefore queries return no matching value.
     */
    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String FindNodeRangeWithValues_type;

    @ParamValues(
            allowed = {"1", "100", "10000", "1000000"},
            base = {"10000"} )
    @Param( {} )
    public int FindNodeRangeWithValues_rangeSize;

    @Override
    public String description()
    {
        return "Tests performance of retrieving nodes by label and property using index range seek.\n" +
                "Method:\n" +
                "- Every node has exactly one, same label\n" +
                "- Every node has exactly one property, with the same property key\n" +
                "- During store creation, property values are assigned with ascending policy, to guaranty uniqueness\n" +
                "- When reading, the index is queries for a range of fixed size with a uniformly random lower bound\n" +
                "Outcome:\n" +
                "- Accesses are spread evenly across the store\n";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        Number initial = defaultRangeFor( FindNodeRangeWithValues_type ).min();
        // will create sequence of ascending number values, starting at 'min' and ending at 'min' + NODE_COUNT
        PropertyDefinition propertyDefinition = ascPropertyFor( FindNodeRangeWithValues_type, initial );
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withLabels( LABEL )
                .withNodeProperties( propertyDefinition )
                .isReusableStore( true )
                .withSchemaIndexes( new LabelKeyDefinition( LABEL, propertyDefinition.key() ) )
                .build();
    }

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return FindNodeRangeWithValues_kernelImplementation;
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        int propertyKey;
        IndexReference index;
        NodeValueIndexCursor node;
        Read read;
        long min;
        long max;
        long rangeSize;
        String type;

        @Setup
        public void setUp( FindNodeRangeWithValues benchmark ) throws Exception
        {
            initializeTx( benchmark );
            Range range = defaultRangeFor( benchmark.FindNodeRangeWithValues_type );
            min = range.min().longValue();
            max = min + NODE_COUNT;
            rangeSize = benchmark.FindNodeRangeWithValues_rangeSize;
            type = benchmark.FindNodeRangeWithValues_type;

            propertyKey = propertyKeyToId( ascPropertyFor( type, min ) );

            index = kernelTx.schemaRead.index( labelToId( LABEL ), propertyKey );
            node = kernelTx.cursors.allocateNodeValueIndexCursor();
            read = kernelTx.read;
        }

        IndexQuery nextQuery( SplittableRandom rng )
        {
            long lowerBound = min + rng.nextLong( max - min - rangeSize );
            long upperBound = lowerBound + rangeSize;
            return IndexQuery.range( propertyKey, value( lowerBound ), true, value( upperBound ), false );
        }

        private Value value( long offset )
        {
            return ascGeneratorFor( type, offset ).create().nextValue( null );
        }

        @TearDown
        public void tearDown() throws Exception
        {
            closeTx();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void findNodeByLabelKeyValue( TxState txState, RNGState rngState, Blackhole bh ) throws KernelException
    {
        IndexQuery query = txState.nextQuery( rngState.rng );
        txState.read.nodeIndexSeek( txState.index, txState.node, IndexOrder.NONE, true, query );
        assertCountAndValues( txState.node, txState.rangeSize, bh );
    }

    public static void main( String... methods ) throws Exception
    {
        run( FindNodeRangeWithValues.class, methods );
    }
}
