/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.Main;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import org.neo4j.internal.collector.ConcurrentLinkedQueueRecentBuffer;
import org.neo4j.internal.collector.RecentBuffer;
import org.neo4j.internal.collector.RingRecentBuffer;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( false )
@OutputTimeUnit( MICROSECONDS )
public class RecentBufferBenchmark extends BaseRegularBenchmark
{
    @Override
    public String description()
    {
        return "Benchmarks RecentBuffer implementations";
    }

    @Override
    public String benchmarkGroup()
    {
        return "Cypher";
    }

    @ParamValues(
            allowed = {"concurrentLinkedQueue", "ringBuffer"},
            base = {"concurrentLinkedQueue", "ringBuffer"} )
    @Param( {} )
    public String impl;

    private RecentBuffer<Long> buffer;

    @Override
    protected void benchmarkSetup( BenchmarkGroup group,
                                   com.neo4j.bench.model.model.Benchmark benchmark,
                                   Neo4jConfig neo4jConfig,
                                   ForkDirectory forkDirectory )
    {
        switch ( impl )
        {
        case "concurrentLinkedQueue":
            buffer = new ConcurrentLinkedQueueRecentBuffer<>( 8192 );
            break;
        case "ringBuffer":
            buffer = new RingRecentBuffer<>( 8192, discarded -> {} );
            break;
        default:
            throw new IllegalStateException( "Unknown thingy" );
        }
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @State( Scope.Thread )
    public static class ThreadState
    {
        RecentBuffer<Long> buffer;

        @Setup
        public void setUp( RecentBufferBenchmark benchmarkState, RNGState rngState ) throws InterruptedException
        {
            buffer = benchmarkState.buffer;
        }
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.SampleTime} )
    public void produce( ThreadState threadState, RNGState rngState )
    {
        threadState.buffer.produce( rngState.rng.nextLong() );
    }

    public static void main( String[] args ) throws Exception
    {
        Main.run( RecentBufferBenchmark.class );
    }
}
