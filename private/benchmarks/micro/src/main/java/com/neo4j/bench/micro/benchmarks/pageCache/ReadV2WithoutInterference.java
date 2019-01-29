package com.neo4j.bench.micro.benchmarks.pageCache;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.ThreadParams;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

@BenchmarkEnabled( true )
public class ReadV2WithoutInterference extends ReadV2
{
    @State( Scope.Thread )
    public static class Cursor extends CursorState
    {
        @Setup
        public void setUp( ThreadParams threadParams, ReadV2WithoutInterference benchmarkState ) throws IOException
        {
            super.setUp( threadParams, benchmarkState );
        }

        @TearDown
        public void tearDown() throws IOException
        {
            super.tearDown();
        }

        @Override
        public int getPageFlags()
        {
            return PagedFile.PF_SHARED_READ_LOCK;
        }
    }

    @ParamValues(
            allowed = {"0.01", "0.25", "0.5", "0.75", "1.1"},
            base = {"0.01", "1.1"} )
    @Param( {} )
    public double ReadV2WithoutInterference_percentage;

    @Override
    public String description()
    {
        return "Read, data does not fit in cache.";
    }

    @Override
    protected double getPercentageCached()
    {
        return ReadV2WithoutInterference_percentage;
    }

    @Benchmark
    @BenchmarkMode( {Mode.Throughput} )
    public void randomRead( Cursor cursorState, RNGState rngState ) throws IOException
    {
        super.randomRead( cursorState, rngState );
    }
}
