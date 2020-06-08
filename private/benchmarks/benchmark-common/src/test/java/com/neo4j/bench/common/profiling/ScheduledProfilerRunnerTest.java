/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.ImmutableList;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.Benchmark.Mode;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

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
        scheduledProfilers.start( forkDirectory, benchmarkGroup, benchmark, Parameters.NONE, jvm, new Pid( 1111 ) );
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
        CompletableFuture<List<Tick>> futureTicks = new CompletableFuture<>();
        List<ExternalProfiler> externalProfilers = Arrays.asList( new SimpleScheduledProfiler( futureTicks, 5 ) );
        ScheduledProfilerRunner scheduledProfilers = ScheduledProfilerRunner.from( externalProfilers );
        Jvm jvm = Jvm.defaultJvmOrFail();
        // when
        scheduledProfilers.start(
                forkDirectory,
                benchmarkGroup,
                benchmark,
                Parameters.NONE,
                jvm,
                new Pid( 1111 ) );
        try
        {
            // then, wait double time of default fixed rate,
            // to make sure scheduled profiler gets called
            assertArrayEquals( new long[] {0,1,2,3,4}, futureTicks.get( 1, TimeUnit.MINUTES).stream().mapToLong( Tick::counter ).toArray());
        }
        finally
        {
            scheduledProfilers.stop();
        }
        assertFalse( scheduledProfilers.isRunning() );
    }

    public static class SimpleScheduledProfiler implements ExternalProfiler, ScheduledProfiler
    {
        private final List<Tick> ticks;
        private final CompletableFuture<List<Tick>> futureTicks;
        private int waitUntil;

        public SimpleScheduledProfiler( CompletableFuture<List<Tick>> futureTicks, int waitUntil )
        {
            this.ticks = new ArrayList<Tick>();
            this.futureTicks = futureTicks;
            this.waitUntil = waitUntil;
        }

        @Override
        public List<String> invokeArgs( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                Parameters additionalParameters )
        {
            return Collections.emptyList();
        }

        @Override
        public JvmArgs jvmArgs( JvmVersion jvmVersion, ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup,
                Benchmark benchmark, Parameters additionalParameters, Resources resources )
        {
            return JvmArgs.empty();
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
        public void processFailed( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                Parameters additionalParameters )
        {
        }

        @Override
        public void onSchedule( Tick tick, ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
                Parameters additionalParameters, Jvm jvm, Pid pid )
        {
            ticks.add( tick );
            if ( ticks.size() == waitUntil )
            {
                futureTicks.complete( ImmutableList.copyOf( ticks ));
            }
        }
    }
}
