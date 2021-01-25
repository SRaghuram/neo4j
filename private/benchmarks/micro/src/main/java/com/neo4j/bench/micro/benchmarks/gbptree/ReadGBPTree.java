/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;

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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Random;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static com.neo4j.bench.micro.Main.run;

public class ReadGBPTree extends AbstractGBPTreeBenchmark
{
    @ParamValues(
            allowed = {"0", "1000000", "10000000", "100000000"},
            base = {"100000000"} )
    @Param( {} )
    public long initialTreeSize;

    @ParamValues(
            allowed = {"FIXED", "DYNAMIC"},
            base = {"FIXED", "DYNAMIC"} )
    @Param( {} )
    public Layout layoutType;

    @ParamValues(
            allowed = {"8", "256"},
            // TODO re-enable 256 key size after Anton fixes the issue
            // TODO or perhaps 128 would make more sense?
            // base = {"8", "256"} )
            base = {"8"} )
    @Param( {} )
    public int keySize;

    @ParamValues(
            allowed = {"0", "8"},
            base = {"0", "8"} )
    @Param( {} )
    public int valueSize;

    @Override
    public String description()
    {
        return "Benchmark read performance.\n" +
               "Given a tree with random data, access it through full scan & exact lookup on random key.\n" +
               "Purpose: measure seek performance on exact lookups & full scan (most extreme case of range seek).\n" +
               "No concurrent writes.";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    Layout layout()
    {
        return layoutType;
    }

    @Override
    int keySize()
    {
        return keySize;
    }

    @Override
    int valueSize()
    {
        return valueSize;
    }

    @Override
    long initialTreeSize()
    {
        return initialTreeSize;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .build();
    }

    @State( Scope.Thread )
    public static class SeekerState
    {
        private static final long STRIDE = 10000;
        private static int SEEKER_COUNT;
        private AdaptableKey from;
        private AdaptableKey to;
        private GBPTree<AdaptableKey,AdaptableValue> gbpTree;
        private Random random;
        private long position;
        private long initialTreeSize;

        @Setup
        public void setUp( ReadGBPTree benchmarkState ) throws IOException
        {
            initialTreeSize = benchmarkState.initialTreeSize();
            position = (SEEKER_COUNT++ * STRIDE) % initialTreeSize;
            random = randomSequence( position );
            from = benchmarkState.layout.newKey();
            to = benchmarkState.layout.newKey();
            gbpTree = benchmarkState.gbpTree;
        }

        long nextRandom()
        {
            position++;
            if ( position >= initialTreeSize )
            {
                position = 0;
                random = randomSequence( 0 );
            }
            return random.nextLong();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.Throughput} )
    public long readExact( ReadGBPTree.SeekerState seekerState, Blackhole bh ) throws IOException
    {
        long exactMatch = seekerState.nextRandom();
        layout.keyWithSeed( seekerState.from, exactMatch );
        layout.keyWithSeed( seekerState.to, exactMatch );
        long count = 0;
        try ( Seeker<AdaptableKey,AdaptableValue> seek = seekerState.gbpTree.seek( seekerState.from, seekerState.to, PageCursorTracer.NULL ) )
        {
            while ( seek.next() )
            {
                bh.consume( seek.key() );
                count++;
            }
        }
        return assertCount( count, 1 );
    }

    @Benchmark
    @BenchmarkMode( {Mode.Throughput} )
    public long readFullScan( ReadGBPTree.SeekerState seekerState, Blackhole bh ) throws IOException
    {
        layout.keyWithSeed( seekerState.from, Long.MIN_VALUE );
        layout.keyWithSeed( seekerState.to, Long.MAX_VALUE );
        long count = 0;
        try ( Seeker<AdaptableKey,AdaptableValue> seek = seekerState.gbpTree.seek( seekerState.from, seekerState.to, PageCursorTracer.NULL ) )
        {
            while ( seek.next() )
            {
                bh.consume( seek.key() );
                count++;
            }
        }
        return assertCount( count, initialTreeSize );
    }

    public static void main( String... methods ) throws Exception
    {
        run( ReadGBPTree.class, methods );
    }
}
