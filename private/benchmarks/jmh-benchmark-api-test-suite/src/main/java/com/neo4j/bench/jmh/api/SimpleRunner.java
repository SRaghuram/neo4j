/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.common.util.Jvm;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.List;

public class SimpleRunner extends Runner
{
    private final int forkCount;
    private final int iterations;
    private final TimeValue duration;

    SimpleRunner( int forkCount, int iterations, TimeValue duration )
    {
        this.forkCount = forkCount;
        this.iterations = iterations;
        this.duration = duration;
    }

    @Override
    protected List<BenchmarkDescription> prepare( List<BenchmarkDescription> benchmarks,
                                                  RunnerParams runnerParams,
                                                  Jvm jvm,
                                                  ErrorReporter errorReporter,
                                                  String[] jvmArgs )
    {
        return benchmarks;
    }

    @Override
    protected ChainedOptionsBuilder beforeProfilerRun( BenchmarkDescription benchmark,
                                                       ProfilerType profilerType,
                                                       RunnerParams runnerParams,
                                                       ChainedOptionsBuilder optionsBuilder )
    {
        return augmentOptions( optionsBuilder );
    }

    @Override
    protected void afterProfilerRun( BenchmarkDescription benchmark, ProfilerType profilerType, RunnerParams runnerParams, ErrorReporter errorReporter )
    {

    }

    @Override
    protected ChainedOptionsBuilder beforeMeasurementRun( BenchmarkDescription benchmark,
                                                          RunnerParams runnerParams,
                                                          ChainedOptionsBuilder optionsBuilder )
    {
        return augmentOptions( optionsBuilder );
    }

    @Override
    protected void afterMeasurementRun( BenchmarkDescription benchmark, RunnerParams runnerParams, ErrorReporter errorReporter )
    {

    }

    @Override
    protected Neo4jConfig systemConfigFor( BenchmarkGroup group, Benchmark benchmark, RunnerParams runnerParams )
    {
        return Neo4jConfig.empty();
    }

    @Override
    protected RunnerParams runnerParams( RunnerParams runnerParams )
    {
        // NOTE: 'foo' is declared in BaseSimpleBenchmark
        return runnerParams.copyWithParam( "foo", "bar" );
    }

    private ChainedOptionsBuilder augmentOptions( ChainedOptionsBuilder optionsBuilder )
    {
        return optionsBuilder
                .forks( forkCount )
                .warmupIterations( iterations )
                .warmupTime( duration )
                .measurementIterations( iterations )
                .measurementTime( duration );
    }
}
