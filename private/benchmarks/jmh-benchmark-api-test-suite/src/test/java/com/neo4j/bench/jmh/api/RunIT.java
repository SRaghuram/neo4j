/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.client.model.TestRunError;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.jmh.api.config.Annotations;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RunIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldRunFromConfigFile() throws IOException
    {
        Path benchmarkConfig = temporaryFolder.newFile().toPath();
        Path workDir = temporaryFolder.newFolder().toPath();
        Path profilerRecordingsOutputDir = temporaryFolder.newFolder().toPath();

        Annotations annotations = new Annotations( "com.neo4j.bench.jmh.api" );
        SuiteDescription defaultSuiteDescription = SuiteDescription.fromAnnotations( annotations, new Validation() );
        BenchmarkConfigFile.write(
                defaultSuiteDescription,
                ImmutableSet.of( SimpleBenchmark.class.getName() ),
                false,
                false,
                benchmarkConfig );

        ErrorReporter errorReporter = new ErrorReporter( ErrorReporter.ErrorPolicy.SKIP );

        SimpleRunner simpleRunner = new SimpleRunner( 1, 1, TimeValue.seconds( 1 ) );

        SuiteDescription suiteDescription = Runner.createSuiteDescriptionFor( "com.neo4j.bench.jmh.api", benchmarkConfig );

        String[] jvmArgs = {};
        String[] jmhArgs = {};

        BenchmarkGroupBenchmarkMetrics results = simpleRunner.run(
                suiteDescription,
                Lists.newArrayList( ProfilerType.JFR ),
                jvmArgs,
                new int[]{1},
                workDir,
                errorReporter,
                jmhArgs,
                Jvm.defaultJvm(),
                profilerRecordingsOutputDir );

        List<BenchmarkGroupBenchmark> benchmarks = results.benchmarkGroupBenchmarks();
        assertThat( benchmarks.size(), equalTo( 4 ) );

        List<String> expectedBenchmarkNames = new ArrayList<>();
        for ( Class benchmark : annotations.getBenchmarks() )
        {
            for ( BenchmarkDescription benchmarkDescription : BenchmarkDescription.of( benchmark, new Validation(), annotations ).explode() )
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

        // Check that the correct profiler recordings are created
        long jfrRecordingType = ProfilerRecordingsTestUtil.recordingCountIn( profilerRecordingsOutputDir, RecordingType.JFR );
        long expectedJfrRecordingCount = suiteDescription.benchmarks().stream().mapToLong( b -> b.explode().size() ).sum();
        assertThat( jfrRecordingType, equalTo( expectedJfrRecordingCount ) );
    }

    @Test
    public void shouldRunFromSingleBenchmark() throws IOException
    {
        Path workDir = temporaryFolder.newFolder().toPath();
        Path profilerRecordingsOutputDir = temporaryFolder.newFolder().toPath();

        ErrorReporter errorReporter = new ErrorReporter( ErrorReporter.ErrorPolicy.SKIP );

        SimpleRunner simpleRunner = new SimpleRunner( 1, 1, TimeValue.seconds( 1 ) );

        SuiteDescription suiteDescription = Runner.createSuiteDescriptionFor( SimpleBenchmark.class, "count" );

        String[] jvmArgs = {};
        String[] jmhArgs = {};

        BenchmarkGroupBenchmarkMetrics results = simpleRunner.run(
                suiteDescription,
                Lists.newArrayList( ProfilerType.JFR ),
                jvmArgs,
                new int[]{1},
                workDir,
                errorReporter,
                jmhArgs,
                Jvm.defaultJvm(),
                profilerRecordingsOutputDir );

        List<BenchmarkGroupBenchmark> benchmarks = results.benchmarkGroupBenchmarks();
        assertThat( benchmarks.size(), equalTo( 2 ) );
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

        // Check that the correct profiler recordings are created
        long jfrRecordingType = ProfilerRecordingsTestUtil.recordingCountIn( profilerRecordingsOutputDir, RecordingType.JFR );
        long expectedJfrRecordingCount = suiteDescription.benchmarks().stream().mapToLong( b -> b.explode().size() ).sum();
        assertThat( jfrRecordingType, equalTo( expectedJfrRecordingCount ) );
    }
}
