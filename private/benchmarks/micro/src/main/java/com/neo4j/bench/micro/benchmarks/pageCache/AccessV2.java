/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.pageCache;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.micro.benchmarks.RNGState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * This benchmark is measuring the performance of bounds checking.
 * Other operations, such as copyTo and zapPage, could be added here in the future.
 */
@BenchmarkEnabled( true )
public class AccessV2 extends AbstractPageCacheBenchmarkV2
{
    @Override
    protected long getFileSize()
    {
        return ByteUnit.kibiBytes( 8 );
    }

    @Override
    public String description()
    {
        return "Single threaded memory accesses via a page cursor, without pin or unpin.";
    }

    @Override
    protected double getPercentageCached()
    {
        return 60; // 60 pages of 8 KiB each (a value of 1 would be caching 100% of the file, which is 8 KiB)
    }

    @Override
    public boolean isThreadSafe()
    {
        // We don't care about multi-threading here. It would only increase observed variance.
        return false;
    }

    @State( Scope.Thread )
    public static class CursorState
    {
        private PageCursor pageCursor;

        @Setup
        public void setUp( AccessV2 benchmarkState ) throws IOException
        {
            PagedFile pagedFile = benchmarkState.pagedFile;
            pageCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, PageCursorTracer.NULL );
            pageCursor.next();
        }

        @TearDown
        public void tearDown()
        {
            pageCursor.close();
        }
    }

    private int next( RNGState rngState )
    {
        // Truncate a random number to be within page bounds.
        // The high-order bits have the most entropy when using Linear Congruential Generator PRNGs,
        // hence shift instead of mask.
        return rngState.rng.nextInt() >>> 24;
    }

    // Baseline for all page access benchmarks
    @Benchmark
    @BenchmarkMode( Mode.AverageTime )
    public PageCursor baseline( CursorState cursorState, RNGState rngState )
    {
        PageCursor cursor = cursorState.pageCursor;
        cursor.setOffset( next( rngState ) );
        return cursor;
    }

    // We don't measure short or int access, because their implementations are almost identical to that of long,
    // except less extreme in some sense.
    // Having byte and long access as two data points is good enough.

    @Benchmark
    @BenchmarkMode( Mode.AverageTime )
    public byte getByte( CursorState cursorState, RNGState rngState )
    {
        return baseline( cursorState, rngState ).getByte();
    }

    @Benchmark
    @BenchmarkMode( Mode.AverageTime )
    public long getLong( CursorState cursorState, RNGState rngState )
    {
        return baseline( cursorState, rngState ).getLong();
    }

    @Benchmark
    @BenchmarkMode( Mode.AverageTime )
    public byte getByteAtOffset( CursorState cursorState, RNGState rngState )
    {
        return baseline( cursorState, rngState ).getByte( 0 );
    }

    @Benchmark
    @BenchmarkMode( Mode.AverageTime )
    public long getLongAtOffset( CursorState cursorState, RNGState rngState )
    {
        return baseline( cursorState, rngState ).getLong( 0 );
    }
}
