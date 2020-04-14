/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.benchmarks.Throttler;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.SplittableRandom;
import java.util.stream.IntStream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.nonContendingStridingFor;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@BenchmarkEnabled( true )
public class ConcurrentReadWriteLabels extends AbstractKernelBenchmark
{
    private static final int NODE_COUNT = 100_000;

    private static final String GROUP_NAME = "readWrite";
    // ratio reading (label scan) threads to writing (remove/add label) threads: (write) 4:1 (read)
    private static final int WRITE_RATIO = 4;
    private static final int READ_RATIO = 1;

    private static final int TARGET_WRITE_THROUGHPUT = 1000; // ops/second/thread

    // number of nodes found by GraphDatabaseService::findNodes should be within 20% of expected value
    // tolerance is necessary because some writing threads may be ahead of others
    private static final double RESULT_COUNT_ACCURACY_TOLERANCE = 0.2;

    @ParamValues(
            allowed = {"1", "10", "100", "1000"},
            base = {"100"} )
    @Param( {} )
    public int txSize;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @ParamValues(
            allowed = {"4", "64"},
            base = {"4", "64"} )
    @Param( {} )
    public int count;

    /**
     * In more detail:
     * - Writing threads work on node ID sequences
     * - Sequence of every thread is guaranteed to never overlap with that of another thread
     * - Every thread starts at different offset (to accelerate warmup) in range, then wraps at max
     * - At sequence beginning threads add/remove labels at specific offsets of 'label chain'
     * - At each following node (in sequence) the offsets to add/remove from are incremented by one
     * - When a sequence wraps the add/remove offsets are reset to their initial values, plus one.
     * This guarantees that every node has exactly one label 'missing' from its 'chain' at any time.
     * Last label deleted for a node is the next label added, but only after all nodes have been seen.
     * Outcome:
     * - All nodes have the same number of labels
     * - Number of labels on each node is stable throughout the experiment
     * - The set of labels between any two nodes may differ by at most two
     * - Each label is on (approx) same number of nodes --> every read does (approx) same amount of work
     */
    @Override
    public String description()
    {
        return "Tests performance of retrieving nodes by label, in the presence of updates.\n" +
               "Ratio of read threads to write threads is (read) " + READ_RATIO + ":" + WRITE_RATIO + " (write).\n" +
               "Reads call Read::nodeLabelScan with random label.\n" +
               "Writes call Write::nodeRemoveLabel/nodeAddLabel.\n" +
               "Benchmark invariants:\n" +
               "- All nodes have the same number of labels\n" +
               "- Number of labels on each node is stable throughout the experiment\n" +
               "- The set of labels between any two nodes may differ by at most two\n" +
               "- Each label is on (almost exactly) same number of nodes --> every read does same amount of work";
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
                .withLabels( labels() )
                .withLabelOrder( Order.ORDERED )
                .withNeo4jConfig( Neo4jConfigBuilder.empty().withSetting( record_format, format ).build() )
                .isReusableStore( false )
                .build();
    }

    private Label[] labels()
    {
        return IntStream.range( 0, count ).boxed()
                        .map( i -> Label.label( "Label" + i ) )
                        .toArray( Label[]::new );
    }

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernelImplementation;

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return kernelImplementation;
    }

    @State( Scope.Thread )
    public static class WriteTxState extends TxState
    {
        Throttler throttler;
        ValueGeneratorFun<Long> ids;
        int[] labels;

        int initialCreateLabelId;
        int createLabelId;
        int deleteLabelId;

        @Setup
        public void setUp( ThreadParams threadParams, ConcurrentReadWriteLabels benchmarkState, RNGState rngState )
                throws KernelException
        {
            initializeTx( benchmarkState, benchmarkState.txSize );
            throttler = new Throttler( TARGET_WRITE_THROUGHPUT );
            int threads = threadCountForSubgroupInstancesOf( threadParams );
            int thread = uniqueSubgroupThreadIdFor( threadParams );
            ids = nonContendingStridingFor(
                    LNG,
                    threads,
                    thread,
                    NODE_COUNT ).create();
            labels = labelsToIds( benchmarkState.labels() );
            // set to 'thread' so threads start at different offsets/labels
            initialCreateLabelId = thread;
            createLabelId = initialCreateLabelId;
            updateLabels();
            advanceStoreToStableState( rngState.rng );
        }

        /**
         * Performs one pass of thread's node ID sequence, i.e., visits every node that it owns once.
         * At each node it visits it adds one label from 'labels[]' and removes the label next 'label[]' index.
         * The label it add is already there, as nodes start with all 'labels[]' labels.
         * The label it removes is actually removed.
         * When the loop is complete the number of labels on each node in the store is equal to labels[].length - 1,
         * which is the stable state, as the actual benchmark workload also ensures this invariant is maintained.
         */
        private void advanceStoreToStableState( SplittableRandom rng )
                throws KernelException
        {
            do
            {
                Write write = advance();
                long nodeId = nodeId( rng );
                write.nodeAddLabel( nodeId, createLabel() );
                write.nodeRemoveLabel( nodeId, deleteLabel() );
                updateLabels();
            }
            while ( !ids.wrapped() );
            createLabelId = ++initialCreateLabelId;
            updateLabels();
        }

        long nodeId( SplittableRandom rng )
        {
            return ids.next( rng );
        }

        int createLabel()
        {
            return labels[createLabelId];
        }

        int deleteLabel()
        {
            return labels[deleteLabelId];
        }

        public Write advance() throws InvalidTransactionTypeKernelException, TransactionFailureException
        {
            kernelTx.advance();
            if ( ids.wrapped() )
            {
                createLabelId = ++initialCreateLabelId;
                updateLabels();
            }
            else
            {
                updateLabels();
            }
            return kernelTx.write;
        }

        private void updateLabels()
        {
            createLabelId = (createLabelId + 1) % labels.length;
            deleteLabelId = (createLabelId + 1) % labels.length;
        }

        @TearDown
        public void tearDown() throws Exception
        {
            closeTx();
        }
    }

    @State( Scope.Thread )
    public static class ReadTxState extends AbstractKernelBenchmark.TxState
    {
        Read read;
        int[] labels;
        int minCount;
        int maxCount;

        @Setup
        public void setUp( ConcurrentReadWriteLabels benchmarkState ) throws KernelException
        {
            initializeTx( benchmarkState );
            read = kernelTx.read;
            labels = labelsToIds( benchmarkState.labels() );
            int expectedCount = (int) Math.round( NODE_COUNT * ((labels.length - 1) / (double) labels.length) );
            minCount = expectedCount - (int) (expectedCount * RESULT_COUNT_ACCURACY_TOLERANCE);
            maxCount = expectedCount + (int) (expectedCount * RESULT_COUNT_ACCURACY_TOLERANCE);
        }

        int label( SplittableRandom rng )
        {
            return labels[rng.nextInt( labels.length )];
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
    @Group( GROUP_NAME )
    @GroupThreads( WRITE_RATIO )
    @BenchmarkMode( {Mode.Throughput} )
    public void createDeleteLabel( WriteTxState writeTxState, RNGState rngState )
            throws KernelException
    {
        writeTxState.throttler.waitForNext();
        Write write = writeTxState.advance();
        long node = writeTxState.nodeId( rngState.rng );
        write.nodeAddLabel( node, writeTxState.createLabel() );
        write.nodeRemoveLabel( node, writeTxState.deleteLabel() );
    }

    /**
     * Note: Mode.SampleTime not used because JMH runs benchmark once for every BenchmarkMode in the Group.
     * Throughput is used for the writing threads, with good reason.
     * To save on build time we do not want this benchmark to run twice.
     */
    @Benchmark
    @Group( GROUP_NAME )
    @GroupThreads( READ_RATIO )
    @BenchmarkMode( {Mode.Throughput} )
    public void readNodesWithLabel( ReadTxState readTxState, RNGState rngState, Blackhole bh )
    {
        int label = readTxState.label( rngState.rng );
        try ( NodeLabelIndexCursor nodeCursor = readTxState.kernelTx.cursors.allocateNodeLabelIndexCursor( NULL ) )
        {
            readTxState.read.nodeLabelScan( label, nodeCursor );
            assertCount( nodeCursor, readTxState.minCount, readTxState.maxCount, bh );
        }
    }

    public static void main( String... methods ) throws Exception
    {
        run( ConcurrentReadWriteLabels.class, methods );
    }
}
