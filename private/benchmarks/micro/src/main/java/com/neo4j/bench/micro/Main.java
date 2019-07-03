/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.BaseBenchmark;
import com.neo4j.bench.jmh.api.Runner;
import com.neo4j.bench.jmh.api.config.JmhOptionsUtil;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Help;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import static com.neo4j.bench.client.util.BenchmarkUtil.tryMkDir;

public class Main
{
    public static void main( String[] args )
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder( "bench" )
                .withDefaultCommand( Help.class )
                .withCommands(
                        ListCommand.class,
                        RunExportCommand.class,
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
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR );
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
            ArrayList<ProfilerType> profilers,
            Path storesDir,
            Path profilerRecordingsOutputDir,
            ErrorReporter.ErrorPolicy errorPolicy,
            Path jvmFile,
            String... methods )
    {
        if ( !Files.exists( storesDir ) )
        {
            System.out.println( "Creating stores directory: " + storesDir.toAbsolutePath() );
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
