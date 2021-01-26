/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.benchmarks.executor.FailingForSingleThreadBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.executor.ValidBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.executor.ValidThreadSafeBenchmark;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.BenchmarksFinder;
import com.neo4j.bench.jmh.api.config.Validation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.profiling.ProfilerType.NO_OP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;

public class ExecutorTest
{
    private static final Jvm JVM = Jvm.defaultJvm();
    private static final String[] JVM_ARGS = {};
    private static final ParameterizedProfiler NO_OP_PROFILER = ParameterizedProfiler.defaultProfiler( NO_OP );
    private static final int[] SINGLE_THREAD = {1};

    private final BenchmarksFinder benchmarksFinder = new BenchmarksFinder( "com.neo4j.bench.jmh.api.benchmarks.executor" );
    private final ErrorReporter errorReporter = new ErrorReporter( ErrorReporter.ErrorPolicy.SKIP );

    @TempDir
    Path temporaryFolder;

    @Test
    public void shouldRunBenchmarkAndCollectResults() throws Exception
    {
        Executor executor = new Executor( JVM, JVM_ARGS, errorReporter, createRunnerParams(), SINGLE_THREAD );
        List<BenchmarkDescription> benchmarks = Collections.singletonList( validBenchmark() );
        List<ParameterizedProfiler> profilers = Collections.singletonList( NO_OP_PROFILER );

        Executor.ExecutionResult executionResult = executor.runWithProfilers( benchmarks, profilers, this::configuration, this::doNothing );

        assertThat( executionResult.successfulBenchmarks(), equalTo( benchmarks ) );
        assertThat( executionResult.runResults(), iterableWithSize( 1 ) );
        assertThat( errorReporter.errors(), empty() );
    }

    @Test
    public void shouldReportFailingBenchmarks() throws Exception
    {
        Executor executor = new Executor( JVM, JVM_ARGS, errorReporter, createRunnerParams(), SINGLE_THREAD );
        List<BenchmarkDescription> benchmarks = Collections.singletonList( failingForSingleThreadBenchmark() );
        List<ParameterizedProfiler> profilers = Collections.singletonList( NO_OP_PROFILER );

        Executor.ExecutionResult executionResult = executor.runWithProfilers( benchmarks, profilers, this::configuration, this::doNothing );

        assertThat( executionResult.successfulBenchmarks(), empty() );
        assertThat( executionResult.runResults(), empty() );
        assertThat( errorReporter.errors(), iterableWithSize( 1 ) );
    }

    @Test
    public void shouldContinueAfterFailureButNotReturnFailingBenchmark() throws Exception
    {
        int[] threadCounts = {1, 2};
        Executor executor = new Executor( JVM, JVM_ARGS, errorReporter, createRunnerParams(), threadCounts );
        List<BenchmarkDescription> benchmarks = Collections.singletonList( failingForSingleThreadBenchmark() );
        List<ParameterizedProfiler> profilers = Collections.singletonList( NO_OP_PROFILER );

        Executor.ExecutionResult executionResult = executor.runWithProfilers( benchmarks, profilers, this::configuration, this::doNothing );

        assertThat( executionResult.successfulBenchmarks(), empty() );
        assertThat( executionResult.runResults(), iterableWithSize( 1 ) );
        assertThat( errorReporter.errors(), iterableWithSize( 1 ) );
    }

    @Test
    public void shouldRunOnlyThreadSafeBenchmarksForMultipleThreads() throws Exception
    {
        int[] threadCounts = {1, 2};
        Executor executor = new Executor( JVM, JVM_ARGS, errorReporter, createRunnerParams(), threadCounts );
        List<BenchmarkDescription> benchmarks = Arrays.asList( validBenchmark(), validThreadSafeBenchmark() );
        List<ParameterizedProfiler> profilers = Collections.singletonList( NO_OP_PROFILER );

        Executor.ExecutionResult executionResult = executor.runWithProfilers( benchmarks, profilers, this::configuration, this::doNothing );

        assertThat( executionResult.successfulBenchmarks(), equalTo( benchmarks ) );
        assertThat( executionResult.runResults(), iterableWithSize( 3 ) );
        assertThat( errorReporter.errors(), empty() );
    }

    private RunnerParams createRunnerParams() throws IOException
    {
        Path workDir = Files.createTempDirectory( temporaryFolder, "work" );
        JmhLifecycleTracker.init( workDir );
        return RunnerParams.create( workDir );
    }

    private BenchmarkDescription validBenchmark()
    {
        return BenchmarkDescription.of( ValidBenchmark.class, new Validation(), benchmarksFinder );
    }

    private BenchmarkDescription validThreadSafeBenchmark()
    {
        return BenchmarkDescription.of( ValidThreadSafeBenchmark.class, new Validation(), benchmarksFinder );
    }

    private BenchmarkDescription failingForSingleThreadBenchmark()
    {
        return BenchmarkDescription.of( FailingForSingleThreadBenchmark.class, new Validation(), benchmarksFinder );
    }

    private ChainedOptionsBuilder configuration( ChainedOptionsBuilder builder, BenchmarkDescription benchmark, ParameterizedProfiler profiler )
    {
        return builder
                .warmupIterations( 0 )
                .measurementIterations( 1 )
                .measurementTime( TimeValue.nanoseconds( 1 ) )
                .verbosity( VerboseMode.SILENT )
                .forks( 0 );
    }

    private void doNothing( BenchmarkDescription benchmark, ParameterizedProfiler profiler )
    {
    }
}
