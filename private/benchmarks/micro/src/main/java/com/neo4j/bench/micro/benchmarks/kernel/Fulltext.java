/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.DiscreteGenerator;
import com.neo4j.bench.micro.data.LabelKeyDefinition;
import com.neo4j.bench.micro.data.PropertyDefinition;
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

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.values.storable.TextValue;
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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class Fulltext extends AbstractKernelBenchmark
{
    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernelImplementation;

    @ParamValues(
            allowed = {STR_SML},
            base = {STR_SML} )
    @Param( {} )
    public String propertyType;

    private TextValue highSelectivityValue;
    private TextValue mediumSelectivityValue;
    private TextValue lowSelectivityValue;

    @Override
    public String description()
    {
        return "Tests performance of searching the fulltext schema indexes.\n" +
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
        DiscreteGenerator.Bucket[] buckets = getBuckets( propertyType );
        highSelectivityValue = (TextValue) Values.of( buckets[0].value() );
        mediumSelectivityValue = (TextValue) Values.of( buckets[1].value() );
        lowSelectivityValue = (TextValue) Values.of( buckets[2].value() );
        PropertyDefinition propertyDefinition = getPropertyDefinition( buckets, propertyType );
        DataGeneratorConfigBuilder builder = new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withLabels( LABEL )
                .withNodeProperties( propertyDefinition )
                .isReusableStore( true );
        return builder.withFulltextNodeSchemaIndexes( new LabelKeyDefinition( LABEL, propertyDefinition.key() ) ).build();
    }

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return kernelImplementation;
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        int highSelectivityMin;
        int highSelectivityMax;
        int mediumSelectivityMin;
        int mediumSelectivityMax;
        int lowSelectivityMin;
        int lowSelectivityMax;

        IndexDescriptor index;
        IndexReadSession indexReadSession;
        NodeValueIndexCursor node;
        Read read;

        @Setup
        public void setUp( Fulltext benchmark ) throws Exception
        {
            initializeTx( benchmark );

            highSelectivityMin = minEstimateFor( expectedHighSelectivityCount() );
            highSelectivityMax = maxEstimateFor( expectedHighSelectivityCount() );
            mediumSelectivityMin = minEstimateFor( expectedMediumSelectivityCount() );
            mediumSelectivityMax = maxEstimateFor( expectedMediumSelectivityCount() );
            lowSelectivityMin = minEstimateFor( expectedLowSelectivityCount() );
            lowSelectivityMax = maxEstimateFor( expectedLowSelectivityCount() );

            index = kernelTx.schemaRead.indexGetForName( "ftsNodes" );
            indexReadSession = kernelTx.read.indexReadSession( index );
            node = kernelTx.cursors.allocateNodeValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE);
            read = kernelTx.read;
        }

        public NodeValueIndexCursor query( String query ) throws Exception
        {
            IndexQuery indexQuery = IndexQuery.fulltextSearch( query );
            read.nodeIndexSeek( indexReadSession, node, unconstrained(), indexQuery );
            return node;
        }

        @TearDown
        public void tearDown() throws Exception
        {
            closeTx();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValueWhenSelectivityHigh( Fulltext.TxState txState, Blackhole bh ) throws Exception
    {
        NodeValueIndexCursor result = txState.query( highSelectivityValue.stringValue() );
        assertCount( result, txState.highSelectivityMin, txState.highSelectivityMax, bh );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValueWhenSelectivityMedium( Fulltext.TxState txState, Blackhole bh ) throws Exception
    {
        NodeValueIndexCursor result = txState.query( mediumSelectivityValue.stringValue() );
        assertCount( result, txState.mediumSelectivityMin, txState.mediumSelectivityMax, bh );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValueWhenSelectivityLow( Fulltext.TxState txState, Blackhole bh ) throws Exception
    {
        NodeValueIndexCursor result = txState.query( lowSelectivityValue.stringValue() );
        try
        {
            assertCount( result, txState.lowSelectivityMin, txState.lowSelectivityMax, bh );
        }
        catch ( Exception e )
        {
            e.addSuppressed( new Exception( "Query failed: '" + lowSelectivityValue.stringValue() + "'."  ) );
            throw e;
        }
    }

    public static void main( String... methods )
    {
        run( Fulltext.class, methods );
    }
}
