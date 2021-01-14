/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.data.DataGenerator.LabelLocality;
import com.neo4j.bench.data.DataGenerator.Order;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.SplittableRandom;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.benchmarks.core.ReadLabel.NODE_COUNT;
import static com.neo4j.bench.micro.benchmarks.core.ReadLabel.labels;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class ReadLabel extends AbstractKernelBenchmark
{
    @ParamValues(
            allowed = {"4", "64"},
            base = {"4"} )
    @Param( {} )
    public int count;

    @ParamValues(
            allowed = {"SCATTERED_BY_NODE", "CO_LOCATED_BY_NODE"},
            base = {"SCATTERED_BY_NODE"} )
    @Param( {} )
    public LabelLocality locality;

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernelImplementation;

    @Override
    public String description()
    {
        return "Tests performance of retrieving labels from nodes that have many labels.\n" +
               "Method:\n" +
               "- Every node has the same labels\n" +
               "- For every node labels are added in the same order\n" +
               "- When looking up node to read labels for, node ID is selected using uniform random policy\n" +
               "- When reading labels on nodes, four different policies are used: first, last, random, all.\n" +
               "--- first: check for existence of label that was written to the node first\n" +
               "--- last: check for existence of label that was written to the node last\n" +
               "--- random: check for existence of random label from node\n" +
               "--- all: retrieve all labels from node";
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
                .withNodeCount( NODE_COUNT )
                .withLabels( labels( count ) )
                .withLabelOrder( Order.ORDERED )
                .withLabelLocality( locality )
                .isReusableStore( true )
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
        NodeCursor node;
        Read read;
        int[] labels;
        int firstLabel;
        int lastLabel;

        @Setup
        public void setUp( ReadLabel benchmark ) throws Exception
        {
            initializeTx( benchmark );
            node = kernelTx.cursors.allocateNodeCursor( NULL );
            read = kernelTx.read;
            labels = Stream.of( labels( benchmark.count ) )
                           .mapToInt( this::labelToId )
                           .toArray();

            firstLabel = labels[0];
            lastLabel = labels[labels.length - 1];
        }

        @TearDown
        public void tearDown() throws Exception
        {
            node.close();
            closeTx();
        }

        int randomLabel( SplittableRandom random )
        {
            return labels[random.nextInt( labels.length )];
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long countLabels( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        txState.read.singleNode( nodeId, txState.node );

        txState.node.next();
        return txState.node.labels().numberOfTokens();
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasRandomLabel( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );
        int randomLabel = txState.randomLabel( rngState.rng );

        txState.read.singleNode( nodeId, txState.node );

        txState.node.next();
        return txState.node.hasLabel( randomLabel );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasFirstLabel( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );

        txState.read.singleNode( nodeId, txState.node );

        txState.node.next();
        return txState.node.hasLabel( txState.firstLabel );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public boolean hasLastLabel( TxState txState, RNGState rngState )
    {
        long nodeId = rngState.rng.nextInt( NODE_COUNT );

        txState.read.singleNode( nodeId, txState.node );

        txState.node.next();
        return txState.node.hasLabel( txState.lastLabel );
    }

    public static void main( String... methods ) throws Exception
    {
        run( ReadLabel.class, methods );
    }
}
