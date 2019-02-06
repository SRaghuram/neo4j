/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.atomic.AtomicLong;

import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

@BenchmarkEnabled( true )
public class CreateTx extends AbstractCoreBenchmark
{
    // Only needs to be large enough to allow each thread to lock a different node
    private static final int NODE_COUNT = 1_000;
    private final AtomicLong nodeId = new AtomicLong( 0 );

    @ParamValues(
            allowed = {"off_heap", "on_heap"},
            base = {"on_heap"} )
    @Param( {} )
    public String CreateTx_txMemory;

    @Override
    public String description()
    {
        return "Tests performance of transaction creation, and taking/releasing of locks.";
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
                .isReusableStore( true )
                .withNeo4jConfig( Neo4jConfig.empty().setTransactionMemory( CreateTx_txMemory ) )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Node node;

        @Setup
        public void setUp( CreateTx benchmarkState ) throws InterruptedException
        {
            try ( Transaction ignore = benchmarkState.db().beginTx() )
            {
                node = benchmarkState.db().getNodeById( benchmarkState.nodeId.getAndIncrement() );
            }
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void createTx()
    {
        try ( Transaction tx = db().beginTx() )
        {
            tx.success();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void createTxTakeReadLock( TxState txState )
    {
        try ( Transaction tx = db().beginTx() )
        {
            tx.acquireReadLock( txState.node );
            tx.success();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void createTxTakeAndReleaseReadLock( TxState txState )
    {
        try ( Transaction tx = db().beginTx() )
        {
            Lock lock = tx.acquireReadLock( txState.node );
            lock.release();
            tx.success();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void createTxTakeWriteLock( TxState txState )
    {
        try ( Transaction tx = db().beginTx() )
        {
            tx.acquireWriteLock( txState.node );
            tx.success();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void createTxTakeAndReleaseWriteLock( TxState txState )
    {
        try ( Transaction tx = db().beginTx() )
        {
            Lock lock = tx.acquireWriteLock( txState.node );
            lock.release();
            tx.success();
        }
    }
}
