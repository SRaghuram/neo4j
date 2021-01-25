/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

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

import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.nonContendingStridingFor;

// TODO rewrite all delete benchmarks as concurrent create/delete to reduce store size and improve robustness?
@BenchmarkEnabled( false )
public class DeleteNodes extends AbstractCoreBenchmark
{
    private static final int NODE_COUNT = 100_000_000;

    @ParamValues(
            // Limit batch size, otherwise store is exhausted too quickly and benchmark fails
            allowed = {"1", "10", "100"},
            base = {"1"} )
    @Param( {} )
    public int txSize;

    @Override
    public String description()
    {
        return "Tests node deletion performance.\n" +
               "Deletes nodes from a pre-generated store, using different transaction batch sizes.\n" +
               "Method:\n" +
               "- Threads work on node ID sequences\n" +
               "- Sequence of every thread is guaranteed to never overlap with that of another thread\n" +
               "- Threads starts at different offsets (to accelerate warmup) in range, and wrap at max (node count)\n" +
               "- If a sequence wraps benchmark will fail, so it is important to use a sufficiently large store\n" +
               "Outcome:\n" +
               "- No two threads will ever delete the same node (avoids deadlocks)";
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
                .isReusableStore( false )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        TxBatch txBatch;
        ValueGeneratorFun<Long> ids;
        long nodeId;

        @Setup
        public void setUp( ThreadParams threadParams, DeleteNodes benchmarkState )
        {
            ids = nonContendingStridingFor(
                    LNG,
                    threadParams.getThreadCount(),
                    threadParams.getThreadIndex(),
                    NODE_COUNT ).create();
            nodeId = -1;
            txBatch = new TxBatch( benchmarkState.db(), benchmarkState.txSize );
        }

        long nodeId()
        {
            return nodeId;
        }

        void advance( SplittableRandom rng )
        {
            txBatch.advance();
            nodeId = ids.next( rng );
            if ( ids.wrapped() )
            {
                throw new RuntimeException(
                        "IDs exhausted, nothing left to delete. Benchmark needs higher Node count" );
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
    public void deleteNode( TxState txState, RNGState rngState )
    {
        txState.advance( rngState.rng );
        txState.txBatch.transaction().getNodeById( txState.nodeId() ).delete();
    }
}
