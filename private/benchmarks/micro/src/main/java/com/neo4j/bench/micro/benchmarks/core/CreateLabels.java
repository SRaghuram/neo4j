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
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
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

import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.nonContendingStridingFor;

@BenchmarkEnabled( true )
public class CreateLabels extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 100_000;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"1", "100"} )
    @Param( {} )
    public int txSize;

    @ParamValues(
            allowed = {"off_heap", "on_heap", "default"},
            base = {"default"} )
    @Param( {} )
    public String txMemory;

    @Override
    public String description()
    {
        return
                "Tests label creation performance.\n" +
                "Creates labels on pre-existing nodes, using varying transaction batch sizes.\n" +
                "Method:\n" +
                "- Every node starts with zero labels\n" +
                "- Threads work on node ID sequences\n" +
                "- Sequence of every thread is guaranteed to never overlap with that of another thread\n" +
                "- Threads start at different offsets (to accelerate warmup) in range, then wrap at max and repeat\n" +
                "- When a sequence wraps the thread moves onto the next label\n" +
                "Outcome:\n" +
                "- No two threads will ever create a label on the same node (avoids deadlocks)\n" +
                "- Every node will have the same labels";
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
                .withNeo4jConfig( Neo4jConfigBuilder.empty().setTransactionMemory( txMemory ).build() )
                .isReusableStore( false )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        TxBatch txBatch;
        ValueGeneratorFun<Long> ids;
        long nodeId;
        Label label;
        int labelId;

        @Setup
        public void setUp( ThreadParams threadParams, CreateLabels benchmarkState )
        {
            ids = nonContendingStridingFor(
                    LNG,
                    threadParams.getThreadCount(),
                    threadParams.getThreadIndex(),
                    NODE_COUNT ).create();
            labelId = 0;
            nodeId = -1;
            label = toLabel( labelId );
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.txSize );
        }

        Label toLabel( int i )
        {
            return Label.label( "Label" + i );
        }

        long nodeId()
        {
            return nodeId;
        }

        Label label()
        {
            return label;
        }

        void advance( SplittableRandom rng )
        {
            txBatch.advance();
            nodeId = ids.next( rng );
            if ( ids.wrapped() )
            {
                label = toLabel( ++labelId );
            }
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
    public void addLabel( TxState txState, RNGState rngState )
    {
        txState.advance( rngState.rng );
        txState.txBatch.transaction().getNodeById( txState.nodeId() ).addLabel( txState.label() );
    }
}
