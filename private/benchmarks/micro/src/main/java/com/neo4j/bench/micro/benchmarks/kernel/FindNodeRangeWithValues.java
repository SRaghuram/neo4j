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

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.memory.EmptyMemoryTracker;
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
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class FindNodeRangeWithValues extends AbstractKernelBenchmark
{
    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernelImplementation;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

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
    public String propertyType;

    @ParamValues(
            allowed = {"1", "100", "10000", "1000000"},
            base = {"10000"} )
    @Param( {} )
    public int rangeSize;

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
        Number initial = defaultRangeFor( propertyType ).min();
        // will create sequence of ascending number values, starting at 'min' and ending at 'min' + NODE_COUNT
        PropertyDefinition propertyDefinition = ascPropertyFor( propertyType, initial );
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
        return kernelImplementation;
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        int propertyKey;
        IndexDescriptor index;
        IndexReadSession indexReadSession;
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
            Range range = defaultRangeFor( benchmark.propertyType );
            min = range.min().longValue();
            max = min + NODE_COUNT;
            rangeSize = benchmark.rangeSize;
            type = benchmark.propertyType;

            propertyKey = propertyKeyToId( ascPropertyFor( type, min ) );

            index = single( kernelTx.schemaRead.index( SchemaDescriptor.forLabel( labelToId( LABEL ), propertyKey ) ) );
            indexReadSession = kernelTx.read.indexReadSession( index );
            node = kernelTx.cursors.allocateNodeValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE);
            read = kernelTx.read;
        }

        PropertyIndexQuery nextQuery( SplittableRandom rng )
        {
            long lowerBound = min + rng.nextLong( max - min - rangeSize );
            long upperBound = lowerBound + rangeSize;
            return PropertyIndexQuery.range( propertyKey, value( lowerBound ), true, value( upperBound ), false );
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
        PropertyIndexQuery query = txState.nextQuery( rngState.rng );
        txState.read.nodeIndexSeek( txState.indexReadSession, txState.node, unorderedValues(), query );
        assertCountAndValues( txState.node, txState.rangeSize, bh );
    }

    public static void main( String... methods ) throws Exception
    {
        run( FindNodeRangeWithValues.class, methods );
    }
}
