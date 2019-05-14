package com.neo4j.bench.jmh.api;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.file.Path;
import java.util.List;

public class SimpleRunner extends Runner
{
    private final int forkCount;
    private final int iterations;
    private final TimeValue duration;

    public SimpleRunner( int forkCount, int iterations, TimeValue duration )
    {
        this.forkCount = forkCount;
        this.iterations = iterations;
        this.duration = duration;
    }

    @Override
    protected List<BenchmarkDescription> prepare( List<BenchmarkDescription> benchmarks,
                                                  Path workDir,
                                                  Jvm jvm,
                                                  ErrorReporter errorReporter,
                                                  String[] jvmArgs )
    {
        return benchmarks;
    }

    @Override
    protected ChainedOptionsBuilder beforeProfilerRun( BenchmarkDescription benchmark,
                                                       ProfilerType profilerType,
                                                       Path workDir,
                                                       ChainedOptionsBuilder optionsBuilder )
    {
        return augmentOptions( optionsBuilder );
    }

    @Override
    protected void afterProfilerRun( BenchmarkDescription benchmark, ProfilerType profilerType, Path workDir, ErrorReporter errorReporter )
    {

    }

    @Override
    protected ChainedOptionsBuilder beforeMeasurementRun( BenchmarkDescription benchmark,
                                                          Path workDir,
                                                          ChainedOptionsBuilder optionsBuilder )
    {
        return augmentOptions( optionsBuilder );
    }

    @Override
    protected void afterMeasurementRun( BenchmarkDescription benchmark, Path workDir, ErrorReporter errorReporter )
    {

    }

    @Override
    protected Neo4jConfig systemConfigFor( BenchmarkGroup group, Benchmark benchmark, Path workDir )
    {
        return Neo4jConfig.empty();
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
