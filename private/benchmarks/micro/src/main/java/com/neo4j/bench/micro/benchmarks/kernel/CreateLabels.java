/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

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

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.ValueGeneratorFun;

import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.nonContendingStridingFor;

@BenchmarkEnabled( true )
public class CreateLabels extends AbstractKernelBenchmark
{
    private static final int NODE_COUNT = 100_000;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"1", "100"} )
    @Param( {} )
    public int CreateLabels_txSize;

    @ParamValues(
            allowed = {"off_heap", "on_heap"},
            base = {"on_heap"} )
    @Param( {} )
    public String CreateLabels_txMemory;

    @Override
    public String description()
    {
        return "Tests label creation performance.\n" +
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
                .withNeo4jConfig( Neo4jConfig.empty().setTransactionMemory( CreateLabels_txMemory ) )
                .isReusableStore( false )
                .build();
    }

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation CreateLabels_kernelImplementation;

    @Override
    protected KernelImplementation kernelImplementation()
    {
        return CreateLabels_kernelImplementation;
    }

    @State( Scope.Thread )
    public static class TxState extends AbstractKernelBenchmark.TxState
    {
        ValueGeneratorFun<Long> ids;
        long nodeId;
        int labelId;

        @Setup
        public void setUp( ThreadParams threadParams, CreateLabels benchmarkState ) throws KernelException
        {
            initializeTx( benchmarkState, benchmarkState.CreateLabels_txSize );
            ids = nonContendingStridingFor(
                    LNG,
                    threadParams.getThreadCount(),
                    threadParams.getThreadIndex(),
                    NODE_COUNT ).create();
            labelId = 0;
            nodeId = -1;
        }

        long nodeId()
        {
            return nodeId;
        }

        Write advance( SplittableRandom rng ) throws InvalidTransactionTypeKernelException, TransactionFailureException
        {
            kernelTx.advance();
            nodeId = ids.next( rng );
            if ( ids.wrapped() )
            {
                labelId++;
            }
            return kernelTx.write;
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
    @BenchmarkMode( {Mode.Throughput} )
    public boolean addLabel( TxState txState, RNGState rngState ) throws KernelException
    {
        Write write = txState.advance( rngState.rng );
        return write.nodeAddLabel( txState.nodeId(), txState.labelId );
    }

    public static void main( String... methods ) throws Exception
    {
        run( CreateLabels.class, methods );
    }
}
