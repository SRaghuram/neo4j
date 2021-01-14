/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.data.DataGenerator.Order;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.data.ValueGeneratorFun;
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
import java.util.stream.IntStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import static com.neo4j.bench.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.data.ValueGeneratorUtil.nonContendingStridingFor;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateDeleteLabels extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 100_000;

    @ParamValues(
            allowed = {"1", "10", "100", "1000"},
            base = {"1", "100"} )
    @Param( {} )
    public int txSize;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @ParamValues(
            allowed = {"off_heap", "on_heap", "default"},
            base = {"default"} )
    @Param( {} )
    public String txMemory;

    @ParamValues(
            allowed = {"4", "64"},
            base = {"4", "64"} )
    @Param( {} )
    public int count;

    /**
     * - Threads work on node ID sequences
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
        return "Tests performance of creating and deleting labels via GraphDatabaseService::removeLabel/addLabel.\n" +
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
                .withNeo4jConfig( Neo4jConfigBuilder
                                          .empty()
                                          .withSetting( record_format, format )
                                          .setTransactionMemory( txMemory )
                                          .build() )
                .isReusableStore( false )
                .build();
    }

    private Label[] labels()
    {
        return IntStream.range( 0, count ).boxed()
                        .map( i -> Label.label( "Label" + i ) )
                        .toArray( Label[]::new );
    }

    @State( Scope.Thread )
    public static class WriteTxState
    {
        TxBatch txBatch;
        ValueGeneratorFun<Long> ids;
        Label[] labels;

        int initialCreateLabelId;
        int createLabelId;
        int deleteLabelId;

        @Setup
        public void setUp( ThreadParams threadParams, CreateDeleteLabels benchmarkState, RNGState rngState )
        {
            int threads = threadCountForSubgroupInstancesOf( threadParams );
            int thread = uniqueSubgroupThreadIdFor( threadParams );
            ids = nonContendingStridingFor(
                    LNG,
                    threads,
                    thread,
                    NODE_COUNT ).create();
            labels = benchmarkState.labels();
            // set to 'thread' so threads start at different offsets/labels
            initialCreateLabelId = thread;
            createLabelId = initialCreateLabelId;
            updateLabels();
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.txSize );
            advanceStoreToStableState( benchmarkState.db(), rngState.rng );
        }

        /**
         * Performs one pass of thread's node ID sequence, i.e., visits every node that it owns once.
         * At each node it visits it adds one label from 'labels[]' and removes the label next 'label[]' index.
         * The label it add is already there, as nodes start with all 'labels[]' labels.
         * The label it removes is actually removed.
         * When the loop is complete the number of labels on each node in the store is equal to labels[].length - 1,
         * which is the stable state.
         */
        private void advanceStoreToStableState( GraphDatabaseService db, SplittableRandom rng )
        {
            do
            {
                txBatch.advance();
                Node node = txBatch.transaction().getNodeById( nodeId( rng ) );
                node.addLabel( createLabel() );
                node.removeLabel( deleteLabel() );
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

        Label createLabel()
        {
            return labels[createLabelId];
        }

        Label deleteLabel()
        {
            return labels[deleteLabelId];
        }

        void advance()
        {
            txBatch.advance();
            if ( ids.wrapped() )
            {
                createLabelId = ++initialCreateLabelId;
                updateLabels();
            }
            else
            {
                updateLabels();
            }
        }

        private void updateLabels()
        {
            createLabelId = (createLabelId + 1) % labels.length;
            deleteLabelId = (createLabelId + 1) % labels.length;
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
    public void createDeleteLabel( WriteTxState writeTxState, RNGState rngState )
    {
        writeTxState.advance();
        Node node = writeTxState.txBatch.transaction().getNodeById( writeTxState.nodeId( rngState.rng ) );
        node.addLabel( writeTxState.createLabel() );
        node.removeLabel( writeTxState.deleteLabel() );
    }
}
