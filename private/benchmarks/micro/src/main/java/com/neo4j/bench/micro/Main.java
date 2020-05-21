/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.BaseBenchmark;
import com.neo4j.bench.jmh.api.Runner;
import com.neo4j.bench.jmh.api.config.JmhOptionsUtil;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.neo4j.bench.common.util.BenchmarkUtil.tryMkDir;

public class Main
{
    private static final Logger LOG = LoggerFactory.getLogger( Main.class );

    public static void main( String[] args )
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder( "bench" )
                .withDefaultCommand( Help.class )
                .withCommands(
                        ListCommand.class,
                        RunReportCommand.class,
                        ScheduleMicroCommand.class,
                        Help.class );

        builder.withGroup( "config" )
               .withDescription( "Creates benchmark configuration file" )
               .withDefaultCommand( ConfigDefaultCommand.class )
               .withCommands(
                       ConfigDefaultCommand.class,
                       ConfigGroupCommand.class,
                       ConfigBenchmarksCommand.class );

        builder.build()
               .parse( args )
               .run();
    }

    /**
     * Convenience method for running benchmarks from a specific benchmark class.
     *
     * @param benchmark the benchmark class from which benchmarks will be run
     * @param methods methods of benchmark class to run, if none are provided all will be run
     * @throws Exception
     */
    public static void run(
            Class<? extends BaseBenchmark> benchmark,
            String... methods )
    {
        Path storesDir = Paths.get( "benchmark_stores" );
        Path profilerRecordingsOutputDir = Paths.get( "profiler_recordings" );
        int forkCount = 1;
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR );
        ErrorReporter.ErrorPolicy errorPolicy = ErrorReporter.ErrorPolicy.FAIL;
        Path jvmFile = null;
        run( benchmark,
             forkCount,
             JmhOptionsUtil.DEFAULT_ITERATION_COUNT,
             JmhOptionsUtil.DEFAULT_ITERATION_DURATION,
             profilers,
             storesDir,
             profilerRecordingsOutputDir,
             errorPolicy,
             jvmFile,
             methods );
    }

    /**
     * Convenience method for running benchmarks from a specific benchmark class.
     *
     * @param benchmark the benchmark class from which benchmarks will be run
     * @param forkCount set to 0 for debugging (e.g., breakpoints), and at least 1 for measurement
     * @param iterationCount number of measurement iterations JMH will run, default is 5
     * @param iterationDuration duration of JMH measurement iterations, default is 5 seconds
     * @param profilers profilers to run with -- profilers run in a fork (different process)
     * @param storesDir directory location of stores (and configurations)
     * @param errorPolicy specifies how to deal with errors (skip vs fail fast)
     * @param methods methods of benchmark class to run, if none are provided all will be run
     */
    public static void run(
            Class<? extends BaseBenchmark> benchmark,
            int forkCount,
            int iterationCount,
            TimeValue iterationDuration,
            List<ParameterizedProfiler> profilers,
            Path storesDir,
            Path profilerRecordingsOutputDir,
            ErrorReporter.ErrorPolicy errorPolicy,
            Path jvmFile,
            String... methods )
    {
        if ( !Files.exists( storesDir ) )
        {
            LOG.debug( "Creating stores directory: " + storesDir.toAbsolutePath() );
            tryMkDir( storesDir );
        }

        // only used in interactive mode, to apply more (normally unsupported) benchmark annotations to JMH configuration
        boolean extendedAnnotationSupport = true;
        BenchmarksRunner runner = new BenchmarksRunner( Neo4jConfigBuilder.withDefaults().build(),
                                                        forkCount,
                                                        iterationCount,
                                                        iterationDuration,
                                                        extendedAnnotationSupport );
        SuiteDescription suiteDescription = Runner.createSuiteDescriptionFor( benchmark, methods );
        String[] jvmArgs = new String[0];
        int[] threadCounts = new int[]{1};
        String[] jmhArgs = new String[0];
        runner.run( suiteDescription,
                    profilers,
                    jvmArgs,
                    threadCounts,
                    storesDir,
                    new ErrorReporter( errorPolicy ),
                    jmhArgs,
                    Jvm.bestEffortOrFail( jvmFile ),
                    profilerRecordingsOutputDir );
    }
}
