/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.Lists;
import com.neo4j.bench.micro.benchmarks.Neo4jBenchmark;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.micro.data.Stores;
import com.neo4j.bench.micro.profile.ProfileDescriptor;
import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Help;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.neo4j.bench.micro.BenchmarksRunner.runBenchmark;

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
            Class<? extends Neo4jBenchmark> benchmark,
            String... methods ) throws Exception
    {
        Path profilesDir = Paths.get( "profiler_recordings" );
        Path storesDir = Paths.get( "benchmark_stores" );
        run(
                benchmark,
                true,
                1,
                ProfileDescriptor.profileTo( profilesDir, Lists.newArrayList( ProfilerType.JFR ) ),
                new Stores( storesDir ),
                ErrorReporter.ErrorPolicy.FAIL,
                null,
                methods );
    }

    /**
     * Convenience method for running benchmarks from a specific benchmark class.
     *
     * @param benchmark the benchmark class from which benchmarks will be run
     * @param generateStoresInFork set to false for debugging -- breakpoints only work when executing in same process
     * @param executeForks set to 0 for debugging (e.g., breakpoints), and at least 1 for measurement
     * @param profileDescriptor profilers to run with -- profilers run in a fork (different process)
     * @param stores directory location of stores (and configurations)
     * @param errorPolicy specifies how to deal with errors (skip vs fail fast)
     * @param methods methods of benchmark class to run, if none are provided all will be run
     * @throws Exception
     */
    public static void run(
            Class<? extends Neo4jBenchmark> benchmark,
            boolean generateStoresInFork,
            int executeForks,
            ProfileDescriptor profileDescriptor,
            Stores stores,
            ErrorReporter.ErrorPolicy errorPolicy,
            Path jvmFile,
            String... methods ) throws Exception
    {
        runBenchmark(
                benchmark,
                profileDescriptor,
                Neo4jConfig.withDefaults(),
                stores,
                generateStoresInFork,
                executeForks,
                errorPolicy,
                Jvm.bestEffortOrFail( jvmFile ),
                methods );
    }
}
