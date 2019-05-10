package com.neo4j.bench.jmh.api;

import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;

import java.nio.file.Path;
import java.util.List;

public class SimpleRunner extends Runner
{
    public static BenchmarkGroupBenchmarkMetrics configureAndRun( Path benchmarkConfig,
                                                                  Path workDir,
                                                                  Path profilerRecordingsOutputDir,
                                                                  List<ProfilerType> profilers,
                                                                  ErrorReporter errorReporter)
    {
        SimpleRunner simpleRunner = new SimpleRunner();

        SuiteDescription suiteDescription = Runner.createSuiteDescriptionFor( "com.neo4j.bench.jmh.api", benchmarkConfig );

        String[] jvmArgs = {};
        String[] jmhArgs = {};

        return simpleRunner.run(
                suiteDescription,
                profilers,
                jvmArgs,
                suiteDescription.benchmarks().stream().mapToInt( x -> 1 ).toArray(),
                workDir,
                errorReporter,
                jmhArgs,
                Jvm.defaultJvm(),
                profilerRecordingsOutputDir );
    }

    @Override
    protected List<BenchmarkDescription> prepare( List<BenchmarkDescription> benchmarks, Path workDir, Jvm jvm, ErrorReporter errorReporter )
    {
        return benchmarks;
    }

    @Override
    protected ChainedOptionsBuilder beforeProfilerRun( BenchmarkDescription benchmark, ProfilerType profilerType, Path workDir, ErrorReporter errorReporter,
                                                       ChainedOptionsBuilder optionsBuilder )
    {
        return optionsBuilder;
    }

    @Override
    protected void afterProfilerRun( BenchmarkDescription benchmark, ProfilerType profilerType, Path workDir, ErrorReporter errorReporter )
    {

    }

    @Override
    protected ChainedOptionsBuilder beforeMeasurementRun( BenchmarkDescription benchmark, Path workDir, ErrorReporter errorReporter,
                                                          ChainedOptionsBuilder optionsBuilder )
    {
        return optionsBuilder;
    }

    @Override
    protected void afterMeasurementRun( BenchmarkDescription benchmark, Path workDir, ErrorReporter errorReporter )
    {

    }
}
