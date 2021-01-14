/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.atomic.AtomicLong;

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
            allowed = {"off_heap", "on_heap", "default"},
            base = {"default"} )
    @Param( {} )
    public String txMemory;

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
                .withNeo4jConfig( Neo4jConfigBuilder.empty().setTransactionMemory( txMemory ).build() )
                .build();
    }

    @State( Scope.Thread )
    public static class TxState
    {
        Node node;

        @Setup
        public void setUp( CreateTx benchmarkState ) throws InterruptedException
        {
            try ( Transaction tx = benchmarkState.db().beginTx() )
            {
                node = tx.getNodeById( benchmarkState.nodeId.getAndIncrement() );
            }
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void createTx()
    {
        try ( Transaction tx = db().beginTx() )
        {
            tx.commit();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void createTxTakeReadLock( TxState txState )
    {
        try ( Transaction tx = db().beginTx() )
        {
            tx.acquireReadLock( txState.node );
            tx.commit();
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
            tx.commit();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void createTxTakeWriteLock( TxState txState )
    {
        try ( Transaction tx = db().beginTx() )
        {
            tx.acquireWriteLock( txState.node );
            tx.commit();
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
            tx.commit();
        }
    }
}
