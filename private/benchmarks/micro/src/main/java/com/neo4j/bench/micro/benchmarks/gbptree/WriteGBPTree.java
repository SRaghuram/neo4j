/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;

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
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.Random;

import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static com.neo4j.bench.micro.Main.run;

public class WriteGBPTree extends AbstractGBPTreeBenchmark
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
        return "Benchmark write performance.\n" +
               "Given a tree with random data. Measure insert throughput of random entries.\n" +
               "No concurrent reads.";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
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
                .isReusableStore( false )
                .build();
    }

    @State( Scope.Thread )
    public static class WriterState
    {
        private Writer<AdaptableKey,AdaptableValue> writer;
        private AdaptableKey key;
        private AdaptableValue value;
        private Random random;

        @Setup
        public void setUp( WriteGBPTree benchmarkState ) throws IOException
        {
            long initialTreeSize = benchmarkState.initialTreeSize();
            random = randomSequence( initialTreeSize );
            writer = benchmarkState.gbpTree.writer( PageCursorTracer.NULL );
            key = benchmarkState.layout.newKey();
            value = benchmarkState.layout.newValue();
        }

        @TearDown
        public void tearDown() throws IOException
        {
            writer.close();
        }

        long nextRandom()
        {
            return random.nextLong();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.Throughput} )
    public void putRandom( WriterState writerState ) throws IOException
    {
        long seed = writerState.nextRandom();
        layout.keyWithSeed( writerState.key, seed );
        layout.valueWithSeed( writerState.value, seed );
        writerState.writer.put( writerState.key, writerState.value );
    }

    public static void main( String... methods ) throws Exception
    {
        run( WriteGBPTree.class, methods );
    }
}
