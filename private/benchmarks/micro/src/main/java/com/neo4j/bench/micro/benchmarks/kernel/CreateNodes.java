/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;

import static com.neo4j.bench.micro.Main.run;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateNodes extends AbstractKernelBenchmark
{
    private static final String LABEL_NAME = "LABEL";
    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
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

    @ParamValues( allowed = {"records"}, base = "records" )
    @Param( {} )
    public KernelImplementation kernelImplementation;

    @Override
    public String description()
    {
        return "Tests performance of node creation, using different transaction batch sizes";
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
                .withNeo4jConfig( Neo4jConfigBuilder
                                          .empty()
                                          .withSetting( record_format, format )
                                          .setTransactionMemory( txMemory )
                                          .build() )
                .isReusableStore( false )
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

        @Setup
        public void setUp( CreateNodes benchmarkState ) throws InterruptedException, KernelException
        {
            initializeTx( benchmarkState, benchmarkState.txSize );
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
    public long createNode( CreateNodes.TxState txState ) throws TransactionFailureException, InvalidTransactionTypeKernelException
    {
        txState.kernelTx.advance();
        return txState.kernelTx.write.nodeCreate();
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
    public long createNodeWithLabel( CreateNodes.TxState txState ) throws KernelException
    {
        txState.kernelTx.advance();
        long node = txState.kernelTx.write.nodeCreate();
        int token = txState.kernelTx.token.labelGetOrCreateForName( LABEL_NAME );
        txState.kernelTx.write.nodeAddLabel( node, token );
        return node;
    }

    public static void main( String... methods ) throws Exception
    {
        run( CreateNodes.class, methods );
    }
}
