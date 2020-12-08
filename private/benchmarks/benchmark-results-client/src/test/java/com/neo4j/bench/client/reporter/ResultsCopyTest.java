/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.reporter;

import com.google.common.collect.ImmutableMap;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
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
import java.util.HashMap;
import java.util.Map;

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
    private static final String FORK1 = "fork 1";
    private static final Metrics METRICS = new Metrics( SECONDS, 1, 10, 5.0, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    public void shouldAddProfilersToBenchmark() throws Exception
    {
        Path parentDir = temporaryFolder.directory("parent");
        ForkDirectory forkDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP ).findOrCreate( BENCH ).create( FORK1 );
        RecordingDescriptor recordingDescriptor = registerProfiler( forkDir, ParameterizedProfiler.defaultProfiler( ProfilerType.JFR ) );

        BenchmarkGroupBenchmarkMetrics metrics = new BenchmarkGroupBenchmarkMetrics();
        metrics.add( GROUP, BENCH, METRICS, null, new Neo4jConfig() );
        Path targetDir = temporaryFolder.directory("target");

        ResultsCopy.extractProfilerRecordings( metrics, targetDir, new URI( "/" ), parentDir );

        Map<String,String> expectedRecordings = ImmutableMap.of( RecordingType.JFR.propertyKey(), "/" + recordingDescriptor.filename() );
        assertThat( metrics.getMetricsFor( new BenchmarkGroupBenchmark( GROUP, BENCH ) ).profilerRecordings().toMap(), equalTo( expectedRecordings ) );
        assertTrue( Files.exists( targetDir.resolve( recordingDescriptor.filename() ) ), "File was not copied" );
    }

    @Test
    public void shouldIgnoreUnsuccessfulBenchmarks() throws Exception
    {
        Path parentDir = temporaryFolder.directory("parent");
        ForkDirectory forkDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP ).findOrCreate( BENCH ).create( FORK1 );
        RecordingDescriptor recordingDescriptor = registerProfiler( forkDir, ParameterizedProfiler.defaultProfiler( ProfilerType.JFR ) );

        BenchmarkGroupBenchmarkMetrics metrics = new BenchmarkGroupBenchmarkMetrics();
        Path targetDir = temporaryFolder.directory("target");

        ResultsCopy.extractProfilerRecordings( metrics, targetDir, new URI( "/" ), parentDir );

        assertFalse( Files.exists( targetDir.resolve( recordingDescriptor.filename() ) ), "File was copied" );
    }

    private RecordingDescriptor registerProfiler( ForkDirectory forkDir, ParameterizedProfiler profiler )
    {
        ProfilerRecordingDescriptor profilerDescriptor = ProfilerRecordingDescriptor.create( GROUP, BENCH, RunPhase.MEASUREMENT, profiler, Parameters.NONE );
        RecordingDescriptor recordingDescriptor = profilerDescriptor.recordingDescriptorFor( profiler.profilerType().recordingType() );
        forkDir.findOrCreate( recordingDescriptor.sanitizedFilename() );
        forkDir.registerPathFor( recordingDescriptor );
        return recordingDescriptor;
    }
}
