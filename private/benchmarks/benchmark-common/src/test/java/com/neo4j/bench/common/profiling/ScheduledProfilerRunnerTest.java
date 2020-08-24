/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.ImmutableList;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.Benchmark.Mode;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfiler;
import static com.neo4j.bench.common.profiling.ProfilerType.NO_OP;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScheduledProfilerRunnerTest
{
    @Test
    public void startAndStopEmptyProfilers()
    {
        // given
        ScheduledProfilerRunner scheduledProfilerRunner = new ScheduledProfilerRunner();

        // when
        // nothing submitted

        // then
        assertTrue( scheduledProfilerRunner.isRunning() );

        //when
        scheduledProfilerRunner.stop();

        // then
        assertFalse( scheduledProfilerRunner.isRunning() );
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
        ForkDirectory forkDirectory = benchmarkDirectory.findOrCreate( "forkName" );
        CompletableFuture<List<Tick>> futureTicks = new CompletableFuture<>();
        List<ExternalProfiler> externalProfilers = Arrays.asList( new SimpleScheduledProfiler( futureTicks, 2 ) );
        ScheduledProfilerRunner scheduledProfilerRunner = new ScheduledProfilerRunner();
        List<ScheduledProfiler> scheduledProfilers = ScheduledProfilerRunner.toScheduleProfilers( externalProfilers );
        Jvm jvm = Jvm.defaultJvmOrFail();
        // when
        scheduledProfilers.forEach( profiler -> scheduledProfilerRunner.submit( profiler,
                                                                                forkDirectory,
                                                                                ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                                                                                    benchmark,
                                                                                                                    RunPhase.MEASUREMENT,
                                                                                                                    defaultProfiler( NO_OP ), /*not used*/
                                                                                                                    Parameters.NONE ),
                                                                                jvm,
                                                                                new Pid( 1111 ) /*not used*/ ) );
        try
        {
            // then, wait double time of default fixed rate,
            // to make sure scheduled profiler gets called
            assertArrayEquals( new long[]{0, 1}, futureTicks.get( 1, TimeUnit.MINUTES ).stream().mapToLong( Tick::counter ).toArray() );
        }
        finally
        {
            scheduledProfilerRunner.stop();
        }
        assertFalse( scheduledProfilerRunner.isRunning() );
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
        public List<String> invokeArgs( ForkDirectory forkDirectory,
                                        ProfilerRecordingDescriptor profilerRecordingDescriptor )
        {
            return Collections.emptyList();
        }

        @Override
        public JvmArgs jvmArgs( JvmVersion jvmVersion,
                                ForkDirectory forkDirectory,
                ProfilerRecordingDescriptor profilerRecordingDescriptor,
                                Resources resources )
        {
            return JvmArgs.empty();
        }

        @Override
        public void beforeProcess( ForkDirectory forkDirectory,
                                   ProfilerRecordingDescriptor profilerRecordingDescriptor )
        {
        }

        @Override
        public void afterProcess( ForkDirectory forkDirectory,
                                  ProfilerRecordingDescriptor profilerRecordingDescriptor )
        {
        }

        @Override
        public void processFailed( ForkDirectory forkDirectory,
                                   ProfilerRecordingDescriptor profilerRecordingDescriptor )
        {
        }

        @Override
        public void onSchedule( Tick tick,
                                ForkDirectory forkDirectory,
                                ProfilerRecordingDescriptor profilerRecordingDescriptor,
                                Jvm jvm,
                                Pid pid )
        {
            ticks.add( tick );
            if ( ticks.size() == waitUntil )
            {
                futureTicks.complete( ImmutableList.copyOf( ticks ) );
            }
        }
    }
}
