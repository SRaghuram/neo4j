/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.reporter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.profiling.RecordingType;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public class ResultsCopyTest
{
    private static final BenchmarkGroup GROUP = new BenchmarkGroup( "group1" );
    private static final Benchmark BENCH = Benchmark.benchmarkFor( "test bench 1", "bench 1", Benchmark.Mode.LATENCY, new HashMap<>() );
    private static final Benchmark CHILD_1 = Benchmark.benchmarkFor( "test bench 1", "bench 1:child 1", Benchmark.Mode.LATENCY, new HashMap<>() );
    private static final Benchmark CHILD_2 = Benchmark.benchmarkFor( "test bench 1", "bench 1:child 2", Benchmark.Mode.LATENCY, new HashMap<>() );
    private static final String FORK_1 = "fork 1";
    private static final String FORK_2 = "fork 2";
    private static final Metrics METRICS = new Metrics( SECONDS, 1, 10, 5.0, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
    private static final ParameterizedProfiler PROFILER_JFR = ParameterizedProfiler.defaultProfiler( ProfilerType.JFR );
    private static final ParameterizedProfiler PROFILER_ASYNC = ParameterizedProfiler.defaultProfiler( ProfilerType.ASYNC );

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    public void shouldAddProfilersToBenchmark() throws Exception
    {
        Path parentDir = temporaryFolder.directory( "parent" );
        RecordingDescriptor recordingDescriptor = registerProfiler( parentDir );

        BenchmarkGroupBenchmarkMetrics metrics = new BenchmarkGroupBenchmarkMetrics();
        metrics.add( GROUP, BENCH, METRICS, null, new Neo4jConfig() );
        Path targetDir = temporaryFolder.directory( "target" );

        ResultsCopy.extractProfilerRecordings( metrics, targetDir, new URI( "/" ), parentDir );

        Map<String,String> expectedRecordings = ImmutableMap.of( RecordingType.JFR.propertyKey(), "/" + recordingDescriptor.filename() );
        assertThat( metrics.getMetricsFor( new BenchmarkGroupBenchmark( GROUP, BENCH ) ).profilerRecordings().toMap(), equalTo( expectedRecordings ) );
        assertTrue( Files.exists( targetDir.resolve( recordingDescriptor.filename() ) ), "File was not copied" );
    }

    @Test
    public void shouldAddProfilersToSecondaryBenchmark() throws Exception
    {
        Path parentDir = temporaryFolder.directory( "parent" );
        BenchmarkDirectory benchmarkDirectory = BenchmarkGroupDirectory.createAt( parentDir, GROUP ).findOrCreate( BENCH );
        RecordingDescriptor recordingDescriptor1 = registerProfiler( benchmarkDirectory, PROFILER_JFR, FORK_1, Sets.newHashSet( CHILD_1, CHILD_2 ) );
        RecordingDescriptor recordingDescriptor2 = registerProfiler( benchmarkDirectory, PROFILER_ASYNC, FORK_2, Sets.newHashSet( CHILD_1, CHILD_2 ) );

        BenchmarkGroupBenchmarkMetrics metrics = new BenchmarkGroupBenchmarkMetrics();
        metrics.add( GROUP, CHILD_1, METRICS, null, new Neo4jConfig() );
        metrics.add( GROUP, CHILD_2, METRICS, null, new Neo4jConfig() );
        Path targetDir = temporaryFolder.directory( "target" );

        ResultsCopy.extractProfilerRecordings( metrics, targetDir, new URI( "/" ), parentDir );

        Map<String,String> expectedRecordings = ImmutableMap.of(
                RecordingType.JFR.propertyKey(), "/" + recordingDescriptor1.filename(),
                RecordingType.ASYNC.propertyKey(), "/" + recordingDescriptor2.filename()
        );
        assertThat( metrics.getMetricsFor( new BenchmarkGroupBenchmark( GROUP, CHILD_1 ) ).profilerRecordings().toMap(), equalTo( expectedRecordings ) );
        assertThat( metrics.getMetricsFor( new BenchmarkGroupBenchmark( GROUP, CHILD_2 ) ).profilerRecordings().toMap(), equalTo( expectedRecordings ) );
        assertTrue( Files.exists( targetDir.resolve( recordingDescriptor1.filename() ) ), "File was not copied" );
    }

    @Test
    public void shouldIgnoreUnsuccessfulBenchmarks() throws Exception
    {
        Path parentDir = temporaryFolder.directory( "parent" );
        RecordingDescriptor recordingDescriptor = registerProfiler( parentDir );

        BenchmarkGroupBenchmarkMetrics metrics = new BenchmarkGroupBenchmarkMetrics();
        Path targetDir = temporaryFolder.directory( "target" );

        ResultsCopy.extractProfilerRecordings( metrics, targetDir, new URI( "/" ), parentDir );

        assertFalse( Files.exists( targetDir.resolve( recordingDescriptor.filename() ) ), "File was copied" );
    }

    private RecordingDescriptor registerProfiler( Path parentDir )
    {
        BenchmarkDirectory benchmarkDirectory = BenchmarkGroupDirectory.createAt( parentDir, GROUP ).findOrCreate( BENCH );
        return registerProfiler( benchmarkDirectory, PROFILER_JFR, FORK_1, Collections.emptySet() );
    }

    private RecordingDescriptor registerProfiler( BenchmarkDirectory benchmarkDirectory, ParameterizedProfiler profiler, String fork, Set<Benchmark> secondary )
    {
        ForkDirectory forkDir = benchmarkDirectory.create( fork );
        ProfilerRecordingDescriptor profilerDescriptor =
                ProfilerRecordingDescriptor.create( GROUP, BENCH, RunPhase.MEASUREMENT, profiler, Parameters.NONE, secondary );
        RecordingDescriptor recordingDescriptor = profilerDescriptor.recordingDescriptorFor( profiler.profilerType().recordingType() );
        forkDir.findOrCreate( recordingDescriptor.sanitizedFilename() );
        forkDir.registerPathFor( recordingDescriptor );
        return recordingDescriptor;
    }
}
