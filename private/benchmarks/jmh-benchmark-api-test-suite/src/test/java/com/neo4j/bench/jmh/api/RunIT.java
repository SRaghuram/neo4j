/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.google.common.collect.ImmutableSet;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.BenchmarksFinder;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.TestRunError;
import com.neo4j.bench.model.profiling.RecordingType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfilers;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RunIT
{
    private static final String[] EXPECTED_NAMES_CONFIG_FILE =
            {"SimpleBenchmark.count", "SimpleBenchmark.count", "SimpleBenchmark.spin", "SimpleBenchmark.spin"};
    private static final String[] EXPECTED_NAMES_SINGLE_BENCHMARK =
            {"SimpleBenchmark.count", "SimpleBenchmark.count"};
    private static final String[] EXPECTED_NAMES_SINGLE_BENCHMARK_TWO_THREADS =
            {"SimpleBenchmark.count", "SimpleBenchmark.count", "SimpleBenchmark.count", "SimpleBenchmark.count"};

    @TempDir
    Path temporaryFolder;

    @Test
    public void shouldRunFromConfigFileInProcess() throws IOException
    {
        int forkCount = 0;
        shouldRun( forkCount, createConfigFileSuiteDescription(), EXPECTED_NAMES_CONFIG_FILE );
    }

    @Test
    public void shouldRunFromConfigFileForked() throws IOException
    {
        int forkCount = 1;
        shouldRun( forkCount, createConfigFileSuiteDescription(), EXPECTED_NAMES_CONFIG_FILE );
    }

    private SuiteDescription createConfigFileSuiteDescription() throws IOException
    {
        Path benchmarkConfig = Files.createTempFile( temporaryFolder, "benchmarkConfig", ".tmp" );
        String packageName = SimpleBenchmark.class.getPackage().getName();
        BenchmarksFinder benchmarksFinder = new BenchmarksFinder( packageName );
        SuiteDescription defaultSuiteDescription = SuiteDescription.fromAnnotations( benchmarksFinder, new Validation() );
        BenchmarkConfigFile.write(
                defaultSuiteDescription,
                ImmutableSet.of( SimpleBenchmark.class.getName() ),
                false,
                false,
                benchmarkConfig );
        return Runner.createSuiteDescriptionFor( packageName, benchmarkConfig );
    }

    @Test
    public void shouldRunFromSingleBenchmarkInProcess() throws IOException
    {
        int forkCount = 0;
        shouldRun( forkCount, createSingleBenchmarkSuite(), EXPECTED_NAMES_SINGLE_BENCHMARK );
    }

    @Test
    public void shouldRunFromSingleBenchmarkForked() throws IOException
    {
        int forkCount = 1;
        shouldRun( forkCount, createSingleBenchmarkSuite(), EXPECTED_NAMES_SINGLE_BENCHMARK );
    }

    @Test
    public void shouldRunFromSingleBenchmarkForkedManyThreads() throws IOException
    {
        int forkCount = 1;
        int[] threadCounts = {1, 2};
        shouldRun( forkCount, createSingleBenchmarkSuite(), EXPECTED_NAMES_SINGLE_BENCHMARK_TWO_THREADS, threadCounts );
    }

    private SuiteDescription createSingleBenchmarkSuite()
    {
        return Runner.createSuiteDescriptionFor( SimpleBenchmark.class, "count" );
    }

    private void shouldRun( int forkCount, SuiteDescription suiteDescription, String[] expectedNames ) throws IOException
    {
        shouldRun( forkCount, suiteDescription, expectedNames, new int[]{1} );
    }

    private void shouldRun( int forkCount, SuiteDescription suiteDescription, String[] expectedNames, int[] threadCounts ) throws IOException
    {
        Path workDir = Files.createTempDirectory( temporaryFolder, "work" );

        List<ParameterizedProfiler> profilers = (forkCount == 0) ? Collections.emptyList()
                                                                 : defaultProfilers( ProfilerType.JFR );
        ErrorReporter errorReporter = new ErrorReporter( ErrorReporter.ErrorPolicy.SKIP );

        SimpleRunner simpleRunner = new SimpleRunner( forkCount, 1, TimeValue.seconds( 5 ) );

        String[] jvmArgs = {};
        String[] jmhArgs = {};

        BenchmarkGroupBenchmarkMetrics results = simpleRunner.run(
                suiteDescription,
                profilers,
                jvmArgs,
                threadCounts,
                workDir,
                errorReporter,
                jmhArgs,
                Jvm.defaultJvm() );

        List<BenchmarkGroupBenchmark> benchmarks = results.benchmarkGroupBenchmarks();
        List<String> benchmarkNames = benchmarks.stream()
                                                .map( benchmarkGroupBenchmark -> benchmarkGroupBenchmark.benchmark().simpleName() )
                                                .collect( Collectors.toList() );
        assertThat( benchmarkNames, containsInAnyOrder( expectedNames ) );

        for ( BenchmarkGroupBenchmark benchmark : benchmarks )
        {
            assertThat( benchmark.benchmarkGroup().name(), equalTo( "test" ) );
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
            long jfrRecordingType = ProfilerRecordingsTestUtil.recordingCountIn( workDir, RecordingType.JFR );
            long expectedJfrRecordingCount = suiteDescription.benchmarks().stream().mapToInt( b -> b.explode().size() * threadCounts.length ).sum();
            assertThat( jfrRecordingType, equalTo( expectedJfrRecordingCount ) );
        }
    }
}
