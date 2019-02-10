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
import com.neo4j.bench.micro.data.ValueGeneratorFun;
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
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;

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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.ascPropertyFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.defaultRangeFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class FindNodeUnique extends AbstractKernelBenchmark
{
    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation FindNodeUnique_kernelImplementation;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String FindNodeUnique_format;

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
    public String FindNodeUnique_type;

    @Override
    public String description()
    {
        return "Tests performance of retrieving nodes by label and property using index seek.\n" +
               "Method:\n" +
               "- Every node has exactly one, same label\n" +
               "- Every node has exactly one property, same and the property (key)\n" +
               "- During store creation, property values are assigned with ascending policy, to guaranty uniqueness\n" +
               "- When reading, property value generation is uniform random, to spread load across store\n" +
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
        Number initial = defaultRangeFor( FindNodeUnique_type ).min();
        // will create sequence of ascending number values, starting at 'min' and ending at 'min' + NODE_COUNT
        PropertyDefinition propertyDefinition = ascPropertyFor( FindNodeUnique_type, initial );
        DataGeneratorConfigBuilder builder = new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withLabels( LABEL )
                .withNodeProperties( propertyDefinition )
                .isReusableStore( true );

        return builder.withSchemaIndexes( new LabelKeyDefinition( LABEL, propertyDefinition.key() ) ).build();
    }

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return FindNodeUnique_kernelImplementation;
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        int labelId;
        int propertyKey;
        ValueGeneratorFun valueFun;
        IndexReference index;
        IndexReadSession indexReadSession;
        NodeValueIndexCursor node;
        Read read;

        @Setup
        public void setUp( FindNodeUnique benchmark ) throws Exception
        {
            initializeTx( benchmark );
            Range range = defaultRangeFor( benchmark.FindNodeUnique_type );
            PropertyDefinition propertyDefinition = randPropertyFor(
                    benchmark.FindNodeUnique_type,
                    range.min(),
                    range.min().longValue() + NODE_COUNT,
                    // numerical
                    true );

            propertyKey = propertyKeyToId( propertyDefinition );
            labelId = labelToId( LABEL );

            valueFun = propertyDefinition.value().create();
            index = kernelTx.schemaRead.index( labelId, propertyKey );
            indexReadSession = kernelTx.read.indexReadSession( index );
            node = kernelTx.cursors.allocateNodeValueIndexCursor();
            read = kernelTx.read;
        }

        Object nextValue( SplittableRandom rng )
        {
            return valueFun.next( rng );
        }

        @TearDown
        public void tearDown() throws Exception
        {
            closeTx();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long findNodeByLabelKeyValue( TxState txState, RNGState rngState ) throws KernelException
    {
        IndexQuery query = IndexQuery.exact( txState.propertyKey, txState.nextValue( rngState.rng ) );
        txState.read.nodeIndexSeek( txState.indexReadSession, txState.node, IndexOrder.NONE, false, query );
        return assertCursorNotNull( txState.node );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void countNodesWithLabelKeyValue( TxState txState, RNGState rngState, Blackhole bh ) throws KernelException
    {
        IndexQuery query = IndexQuery.exact( txState.propertyKey, txState.nextValue( rngState.rng ) );
        txState.read.nodeIndexSeek( txState.indexReadSession, txState.node, IndexOrder.NONE, false, query );
        assertCount( txState.node, 1, bh );
    }

    public static void main( String... methods ) throws Exception
    {
        run( FindNodeUnique.class, methods );
    }
}
