/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.neo4j.bench.micro.benchmarks.pageCache;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.ThreadParams;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.neo4j.io.pagecache.PagedFile;

@BenchmarkEnabled( true )
public class WriteV2WithInterference extends WriteV2
{
    @State( Scope.Thread )
    public static class Cursor extends CursorState
    {
        @Setup
        public void setUp( ThreadParams threadParams, WriteV2WithInterference benchmarkState ) throws IOException
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
            return PagedFile.PF_SHARED_WRITE_LOCK;
        }
    }

    @ParamValues(
            allowed = {"0.01", "0.25", "0.5", "0.75", "1.1"},
            base = {"0.01", "1.1"} )
    @Param( {} )
    public double WriteV2WithInterference_percentage;

    @Override
    protected double getPercentageCached()
    {
        return WriteV2WithInterference_percentage;
    }

    @Override
    public String description()
    {
        return "Write, data does not fit in cache, and which is being read from by a concurrent reader.";
    }

    @Setup( Level.Iteration )
    public void startReader()
    {
        startInterference( PagedFile.PF_SHARED_READ_LOCK );
    }

    @TearDown( Level.Iteration )
    public void stopReader() throws ExecutionException, InterruptedException
    {
        stopInterference();
    }

    @Benchmark
    @BenchmarkMode( {Mode.Throughput} )
    public void randomWrite( Cursor cursorState, RNGState rngState ) throws IOException
    {
        super.randomWrite( cursorState, rngState );
    }
}
