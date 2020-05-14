/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.google.common.collect.ImmutableSet;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.TestRunError;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.BenchmarksFinder;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfilers;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RunIT
{
    @TempDir
    Path temporaryFolder;

    @Test
    public void shouldRunFromConfigFileInProcess() throws IOException
    {
        int forkCount = 0;
        shouldRunFromConfigFile( forkCount );
    }

    @Test
    public void shouldRunFromConfigFileForked() throws IOException
    {
        int forkCount = 1;
        shouldRunFromConfigFile( forkCount );
    }

    private void shouldRunFromConfigFile( int forkCount ) throws IOException
    {
        Path benchmarkConfig = Files.createTempFile( temporaryFolder, "benchmarkConfig", ".tmp" );
        Path workDir = Files.createTempDirectory( temporaryFolder, "work" );
        Path profilerRecordingsOutputDir = Files.createTempDirectory( temporaryFolder, "recordings" );

        List<ParameterizedProfiler> profilers = (forkCount == 0) ? Collections.emptyList()
                                                                 : defaultProfilers( ProfilerType.JFR );

        BenchmarksFinder benchmarksFinder = new BenchmarksFinder( SimpleBenchmark.class.getPackage().getName() );
        SuiteDescription defaultSuiteDescription = SuiteDescription.fromAnnotations( benchmarksFinder, new Validation() );
        BenchmarkConfigFile.write(
                defaultSuiteDescription,
                ImmutableSet.of( SimpleBenchmark.class.getName() ),
                false,
                false,
                benchmarkConfig );

        ErrorReporter errorReporter = new ErrorReporter( ErrorReporter.ErrorPolicy.SKIP );

        SimpleRunner simpleRunner = new SimpleRunner( forkCount, 1, TimeValue.seconds( 5 ) );

        SuiteDescription suiteDescription = Runner.createSuiteDescriptionFor( SimpleBenchmark.class.getPackage().getName(), benchmarkConfig );

        String[] jvmArgs = {};
        String[] jmhArgs = {};

        BenchmarkGroupBenchmarkMetrics results = simpleRunner.run(
                suiteDescription,
                profilers,
                jvmArgs,
                new int[]{1},
                workDir,
                errorReporter,
                jmhArgs,
                Jvm.defaultJvm(),
                profilerRecordingsOutputDir );

        List<BenchmarkGroupBenchmark> benchmarks = results.benchmarkGroupBenchmarks();
        assertThat( errorReporter.toString(), benchmarks.size(), equalTo( 4 ) );

        List<String> expectedBenchmarkNames = new ArrayList<>();
        for ( Class benchmark : benchmarksFinder.getBenchmarks() )
        {
            for ( BenchmarkDescription benchmarkDescription : BenchmarkDescription.of( benchmark, new Validation(), benchmarksFinder ).explode() )
            {
                // there is only 1 method per benchmark at this point, because explode() was called above
                String methodName = benchmarkDescription.methods().iterator().next().name();
                expectedBenchmarkNames.add( benchmark.getSimpleName() + "." + methodName );
            }
        }

        for ( BenchmarkGroupBenchmark benchmark : benchmarks )
        {
            assertThat( benchmark.benchmarkGroup().name(), equalTo( "test" ) );
            // check that every expected benchmark appears exactly once
            assertTrue( expectedBenchmarkNames.remove( benchmark.benchmark().simpleName() ), () -> "Did not find: " + benchmark.benchmark().simpleName() );

            BenchmarkGroupBenchmarkMetrics.AnnotatedMetrics metrics = results.getMetricsFor( benchmark.benchmarkGroup(), benchmark.benchmark() );

            // profiler recordings are written to the result in a later step
            assertTrue( metrics.profilerRecordings().toMap().isEmpty() );

            assertThat( benchmark.benchmark().parameters().size(), equalTo( 2 ) );
            // 'threads' parameter is common to all benchmarks, it is added by the runner
            assertTrue( benchmark.benchmark().parameters().containsKey( "threads" ) );
            assertTrue( benchmark.benchmark().parameters().containsKey( "range" ) );

            double mean = (double) metrics.metrics().toMap().get( Metrics.MEAN );
            assertThat( mean, greaterThan( 0D ) );
        }

        // Check that no errors occurred
        Supplier<String> errorMessage = () -> errorReporter.errors().stream().map( TestRunError::toString ).collect( joining( "\n" ) );
        assertTrue( errorReporter.errors().isEmpty(), errorMessage );

        // JFR requires a forked process to work
        if ( forkCount > 0 )
        {
            // Check that the correct profiler recordings are created
            long jfrRecordingType = ProfilerRecordingsTestUtil.recordingCountIn( profilerRecordingsOutputDir, RecordingType.JFR );
            long expectedJfrRecordingCount = suiteDescription.benchmarks().stream().mapToLong( b -> b.explode().size() ).sum();
            assertThat( jfrRecordingType, equalTo( expectedJfrRecordingCount ) );
        }
    }

    @Test
    public void shouldRunFromSingleBenchmarkInProcess() throws IOException
    {
        int forkCount = 0;
        shouldRunFromSingleBenchmark( forkCount );
    }

    @Test
    public void shouldRunFromSingleBenchmarkForked() throws IOException
    {
        int forkCount = 1;
        shouldRunFromSingleBenchmark( forkCount );
    }

    private void shouldRunFromSingleBenchmark( int forkCount ) throws IOException
    {
        Path workDir = Files.createTempDirectory( temporaryFolder, "work" );
        Path profilerRecordingsOutputDir = Files.createTempDirectory( temporaryFolder, "recordings" );

        List<ParameterizedProfiler> profilers = (forkCount == 0) ? Collections.emptyList()
                                                                 : defaultProfilers(  ProfilerType.JFR  );

        ErrorReporter errorReporter = new ErrorReporter( ErrorReporter.ErrorPolicy.SKIP );

        SimpleRunner simpleRunner = new SimpleRunner( forkCount, 1, TimeValue.seconds( 5 ) );

        SuiteDescription suiteDescription = Runner.createSuiteDescriptionFor( SimpleBenchmark.class, "count" );

        String[] jvmArgs = {};
        String[] jmhArgs = {};

        BenchmarkGroupBenchmarkMetrics results = simpleRunner.run(
                suiteDescription,
                profilers,
                jvmArgs,
                new int[]{1},
                workDir,
                errorReporter,
                jmhArgs,
                Jvm.defaultJvm(),
                profilerRecordingsOutputDir );

        List<BenchmarkGroupBenchmark> benchmarks = results.benchmarkGroupBenchmarks();
        assertThat( errorReporter.toString(), benchmarks.size(), equalTo( 2 ) );
        for ( BenchmarkGroupBenchmark benchmark : benchmarks )
        {
            assertThat( benchmark.benchmarkGroup().name(), equalTo( "test" ) );
            assertThat( benchmark.benchmark().simpleName(), equalTo( "SimpleBenchmark.count" ) );
            BenchmarkGroupBenchmarkMetrics.AnnotatedMetrics metrics = results.getMetricsFor( benchmark.benchmarkGroup(), benchmark.benchmark() );

            // profiler recordings are written to the result in a later step
            assertTrue( metrics.profilerRecordings().toMap().isEmpty() );

            assertThat( benchmark.benchmark().parameters().size(), equalTo( 2 ) );
            // 'threads' parameter is common to all benchmarks, it is added by the runner
            assertTrue( benchmark.benchmark().parameters().containsKey( "threads" ) );
            assertTrue( benchmark.benchmark().parameters().containsKey( "range" ) );

            double mean = (double) metrics.metrics().toMap().get( Metrics.MEAN );
            assertThat( mean, greaterThan( 0D ) );
        }

        // Check that no errors occurred
        Supplier<String> errorMessage = () -> errorReporter.errors().stream().map( TestRunError::toString ).collect( joining( "\n" ) );
        assertTrue( errorReporter.errors().isEmpty(), errorMessage );

        // JFR requires a forked process to work
        if ( forkCount > 0 )
        {
            // Check that the correct profiler recordings are created
            long jfrRecordingType = ProfilerRecordingsTestUtil.recordingCountIn( profilerRecordingsOutputDir, RecordingType.JFR );
            long expectedJfrRecordingCount = suiteDescription.benchmarks().stream().mapToLong( b -> b.explode().size() ).sum();
            assertThat( jfrRecordingType, equalTo( expectedJfrRecordingCount ) );
        }
    }
}
