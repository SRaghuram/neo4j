/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.Benchmark.Mode;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.ScheduledProfiler;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static java.util.Collections.emptyMap;

public class ScheduledProfilersTest
{
    @Test
    public void startAndStopEmptyProfilers() throws IOException
    {
        // given
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "benchmarkgroup" );
        BenchmarkGroupDirectory benchmarkGroupDirectory = BenchmarkGroupDirectory.createAt( Files.createTempDirectory( "macro" ), benchmarkGroup );

        Benchmark benchmark = Benchmark.benchmarkFor( "description", "simpleName", Mode.LATENCY, emptyMap() );
        BenchmarkDirectory benchmarkDirectory = benchmarkGroupDirectory.findOrCreate( benchmark );

        ForkDirectory forkDirectory = benchmarkDirectory.findOrCreate( "forkName", Collections.emptyList() );

        ScheduledProfilers scheduledProfilers = ScheduledProfilers.from( Collections.emptyList() );

        // when
        scheduledProfilers.start( forkDirectory, benchmarkGroup, benchmark, null, new Pid( 1111 ) );
        // then
        assertTrue( scheduledProfilers.isRunning() );

        //when
        scheduledProfilers.stop();
        // then
        assertFalse( scheduledProfilers.isRunning() );
    }

    @Test
    public void startAndStopOneProfiler() throws Exception
    {
        // given
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "benchmarkgroup" );
        BenchmarkGroupDirectory benchmarkGroupDirectory = BenchmarkGroupDirectory.createAt( Files.createTempDirectory( "macro" ), benchmarkGroup );

        Benchmark benchmark = Benchmark.benchmarkFor( "description", "simpleName", Mode.LATENCY, emptyMap() );
        BenchmarkDirectory benchmarkDirectory = benchmarkGroupDirectory.findOrCreate( benchmark );

        ForkDirectory forkDirectory = benchmarkDirectory.findOrCreate( "forkName", Collections.emptyList() );

        CompletableFuture<Boolean> future = new CompletableFuture<Boolean>();

        List<ExternalProfiler> externalProfilers = Arrays.asList( new SimpleScheduledProfiler( future ) );

        ScheduledProfilers scheduledProfilers = ScheduledProfilers.from( externalProfilers );

        // when
        scheduledProfilers.start(
                forkDirectory,
                benchmarkGroup,
                benchmark,
                new Parameters( Collections.emptyMap() ),
                new Pid( 1111 ) );
        // then, wait double time of default schedule,
        // to make sure scheduled profiler gets called
        try
        {
            assertTrue( future.get( 10, TimeUnit.SECONDS ) );
        }
        finally
        {
            scheduledProfilers.stop();
        }
    }

    public static class SimpleScheduledProfiler implements ExternalProfiler, ScheduledProfiler
    {

        private final CompletableFuture<Boolean> future;

        public SimpleScheduledProfiler( CompletableFuture<Boolean> future )
        {
            this.future = future;
        }

        @Override
        public List<String> invokeArgs( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                Parameters additionalParameters )
        {
            return Collections.emptyList();
        }

        @Override
        public List<String> jvmArgs( JvmVersion jvmVersion, ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup,
                Benchmark benchmark, Parameters additionalParameters )
        {
            return Collections.emptyList();
        }

        @Override
        public void beforeProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                Parameters additionalParameters )
        {
        }

        @Override
        public void afterProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                Parameters additionalParameters )
        {
        }

        @Override
        public void onSchedule( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                Parameters additionalParameters, Pid pid )
        {
            future.complete( true );
        }

    }
}
