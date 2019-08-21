/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.Benchmark.Mode;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
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
import static org.junit.jupiter.api.Assertions.assertEquals;

import static java.util.Collections.emptyMap;

public class ScheduledProfilerRunnerTest
{
    @Test
    public void startAndStopEmptyProfilers() throws IOException
    {
        // given
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "benchmarkgroup" );
        BenchmarkGroupDirectory benchmarkGroupDirectory =
                BenchmarkGroupDirectory.createAt( Files.createTempDirectory( "macro" ), benchmarkGroup );
        Benchmark benchmark = Benchmark.benchmarkFor( "description", "simpleName", Mode.LATENCY, emptyMap() );
        BenchmarkDirectory benchmarkDirectory = benchmarkGroupDirectory.findOrCreate( benchmark );
        ForkDirectory forkDirectory = benchmarkDirectory.findOrCreate( "forkName", Collections.emptyList() );
        ScheduledProfilerRunner scheduledProfilers = ScheduledProfilerRunner.from( Collections.emptyList() );
        Jvm jvm = Jvm.defaultJvmOrFail();
        // when
        scheduledProfilers.start( forkDirectory, benchmarkGroup, benchmark, null, jvm, new Pid( 1111 ) );
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
        BenchmarkGroupDirectory benchmarkGroupDirectory =
                BenchmarkGroupDirectory.createAt( Files.createTempDirectory( "macro" ), benchmarkGroup );
        Benchmark benchmark = Benchmark.benchmarkFor( "description", "simpleName", Mode.LATENCY, emptyMap() );
        BenchmarkDirectory benchmarkDirectory = benchmarkGroupDirectory.findOrCreate( benchmark );
        ForkDirectory forkDirectory = benchmarkDirectory.findOrCreate( "forkName", Collections.emptyList() );
        CompletableFuture<Tick> futureTick = new CompletableFuture<Tick>();
        List<ExternalProfiler> externalProfilers = Arrays.asList( new SimpleScheduledProfiler( futureTick ) );
        ScheduledProfilerRunner scheduledProfilers = ScheduledProfilerRunner.from( externalProfilers );
        Jvm jvm = Jvm.defaultJvmOrFail();
        // when
        scheduledProfilers.start(
                forkDirectory,
                benchmarkGroup,
                benchmark,
                new Parameters( Collections.emptyMap() ),
                jvm,
                new Pid( 1111 ) );
        try
        {
            // then, wait double time of default fixed rate,
            // to make sure scheduled profiler gets called
            assertEquals(  0, futureTick.get( 10, TimeUnit.SECONDS ).counter() );
        }
        finally
        {
            scheduledProfilers.stop();
        }
        assertFalse( scheduledProfilers.isRunning() );
    }

    public static class SimpleScheduledProfiler implements ExternalProfiler, ScheduledProfiler
    {
        private final CompletableFuture<Tick> future;

        public SimpleScheduledProfiler( CompletableFuture<Tick> future )
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
        public void onSchedule( Tick tick, ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                Parameters additionalParameters, Jvm jvm, Pid pid )
        {
            future.complete( tick );
        }
    }
}
