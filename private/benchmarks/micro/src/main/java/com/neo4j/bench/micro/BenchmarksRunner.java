/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.BenchmarkDiscoveryUtils;
import com.neo4j.bench.jmh.api.Executor;
import com.neo4j.bench.jmh.api.Runner;
import com.neo4j.bench.jmh.api.RunnerParams;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.JmhOptionsUtil;
import com.neo4j.bench.micro.data.Stores;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.profiling.ProfilerType.NO_OP;
import static com.neo4j.bench.common.util.BenchmarkUtil.durationToString;

class BenchmarksRunner extends Runner
{
    private static final Logger LOG = LoggerFactory.getLogger( BenchmarksRunner.class );
    private static final String PARAM_NEO4J_CONFIG = "baseNeo4jConfig";

    private final Neo4jConfig baseNeo4jConfig;
    private final int forkCount;
    private final int iterations;
    private final TimeValue duration;
    private final boolean extendedAnnotationSupport;

    /**
     * Constructor for Micro benchmarks implementation of Runner
     *
     * @param baseNeo4jConfig           base Neo4j configuration that will be used to run every benchmark
     * @param forkCount                 number of measurement forks
     * @param iterations                number of benchmark iterations per fork
     * @param duration                  duration of each benchmark iteration
     * @param extendedAnnotationSupport specifies if the JMH configuration should include the values of all JMH annotations present on the benchmark classes.
     *                                  Only true in interactive mode, to apply more benchmark annotations that are normally ignored by this framework.
     */
    BenchmarksRunner( Neo4jConfig baseNeo4jConfig,
                      int forkCount,
                      int iterations,
                      TimeValue duration,
                      boolean extendedAnnotationSupport )
    {
        this.baseNeo4jConfig = baseNeo4jConfig;
        this.forkCount = forkCount;
        this.iterations = iterations;
        this.duration = duration;
        this.extendedAnnotationSupport = extendedAnnotationSupport;
    }

    @Override
    protected List<BenchmarkDescription> prepare( List<BenchmarkDescription> benchmarks,
                                                  RunnerParams runnerParams,
                                                  Jvm jvm,
                                                  ErrorReporter errorReporter,
                                                  String[] jvmArgs,
                                                  int[] threadCounts )
    {
        // Run every benchmark once to create stores -- triggers store generation in benchmark setups
        // Ensures generation does not occur in benchmark setup later, which would, for example, pollute the heap
        logStageHeader( "STORE GENERATION" );

        Instant storeGenerationStart = Instant.now();

        Stores stores = new Stores( runnerParams.workDir() );

        // necessary for robust fork directory creation
        List<ParameterizedProfiler> profilers = Collections.singletonList( ParameterizedProfiler.defaultProfiler( NO_OP ) );
        Executor executor = new Executor( jvm, jvmArgs, errorReporter, runnerParams, threadCounts );
        Executor.ExecutionResult executionResult = executor.runWithProfilers(
                benchmarks,
                profilers,
                ( builder, benchmark, profiler ) -> storeGenerationOptions( builder ),
                ( benchmark, profiler ) -> stores.deleteFailedStores() );
        List<BenchmarkDescription> benchmarksWithStores = executionResult.successfulBenchmarks();
        Instant storeGenerationFinish = Instant.now();
        Duration storeGenerationDuration = Duration.between( storeGenerationStart, storeGenerationFinish );
        // Print details of storage directory
        LOG.info( "Store generation took: {}", durationToString( storeGenerationDuration ) );
        LOG.info( stores.details() );
        return benchmarksWithStores;
    }

    private ChainedOptionsBuilder storeGenerationOptions( ChainedOptionsBuilder builder )
    {
        return builder
                .warmupIterations( 0 )
                .warmupTime( TimeValue.NONE )
                .measurementIterations( 1 )
                .measurementTime( TimeValue.NONE )
                .verbosity( VerboseMode.SILENT )
                .forks( Math.min( forkCount, 1 ) );
    }

    @Override
    protected ChainedOptionsBuilder beforeProfilerRun( BenchmarkDescription benchmark,
                                                       ProfilerType profilerType,
                                                       RunnerParams runnerParams,
                                                       ChainedOptionsBuilder optionsBuilder )
    {
        return augmentOptions( optionsBuilder, benchmark );
    }

    @Override
    protected void afterProfilerRun( BenchmarkDescription benchmark, ProfilerType profilerType, RunnerParams runnerParams, ErrorReporter errorReporter )
    {
        // do nothing
    }

    @Override
    protected ChainedOptionsBuilder beforeMeasurementRun( BenchmarkDescription benchmark,
                                                          RunnerParams runnerParams,
                                                          ChainedOptionsBuilder optionsBuilder )
    {
        return augmentOptions( optionsBuilder, benchmark ).forks( forkCount );
    }

    @Override
    protected void afterMeasurementRun( BenchmarkDescription benchmark, RunnerParams runnerParams, ErrorReporter errorReporter )
    {
        // do nothing
    }

    @Override
    protected Neo4jConfig systemConfigFor( BenchmarkGroup group, Benchmark benchmark, RunnerParams runnerParams )
    {
        return new Stores( runnerParams.workDir() ).neo4jConfigFor( group, benchmark );
    }

    @Override
    protected RunnerParams runnerParams( RunnerParams runnerParams )
    {
        return runnerParams.copyWithParam( PARAM_NEO4J_CONFIG, baseNeo4jConfig.toJson() );
    }

    private ChainedOptionsBuilder augmentOptions( ChainedOptionsBuilder optionsBuilder, BenchmarkDescription benchmark )
    {
        if ( extendedAnnotationSupport )
        {
            Class<?> benchmarkClass = BenchmarkDiscoveryUtils.benchmarkClassForName( benchmark.className() );
            optionsBuilder = JmhOptionsUtil.applyAnnotations( benchmarkClass, optionsBuilder );
        }
        return optionsBuilder
                .warmupIterations( iterations )
                .warmupTime( duration )
                .measurementIterations( iterations )
                .measurementTime( duration );
    }
}
