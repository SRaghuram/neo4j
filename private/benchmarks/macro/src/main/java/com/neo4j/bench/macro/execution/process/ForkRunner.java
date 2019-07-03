/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.BenchmarkDirectory;
import com.neo4j.bench.client.results.BenchmarkGroupDirectory;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.Store;
import com.neo4j.bench.macro.execution.database.PlanCreator;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ForkRunner
{
    private static final com.neo4j.bench.client.model.Neo4jConfig NO_NEO4J_CONFIG = com.neo4j.bench.client.model.Neo4jConfig.empty();

    public static BenchmarkDirectory runForksFor( DatabaseLauncher<?> launcher,
                                                  BenchmarkGroupDirectory groupDir,
                                                  Query query,
                                                  Store store,
                                                  Edition edition,
                                                  Neo4jConfig neo4jConfig,
                                                  List<ProfilerType> profilers,
                                                  Jvm jvm,
                                                  int measurementForkCount,
                                                  TimeUnit unit,
                                                  BenchmarkGroupBenchmarkMetricsPrinter metricsPrinter,
                                                  List<String> jvmArgs,
                                                  Resources resources ) throws ForkFailureException
    {
        BenchmarkDirectory benchmarkDir = groupDir.findOrCreate( query.benchmark() );
        boolean doFork = measurementForkCount != 0;
        try
        {
            // profiler forks: each profiler run in separate fork
            for ( ProfilerType profiler : profilers )
            {
                String forkName = "profiler-fork-" + profiler.name().toLowerCase();
                ForkDirectory forkDirectory = benchmarkDir.create( forkName, singletonList( profiler ) );
                Path neo4jConfigFile = forkDirectory.create( "neo4j.conf" );
                Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );
                RunnableFork profilerFork = fork( launcher,
                                                  query,
                                                  store,
                                                  neo4jConfigFile,
                                                  forkDirectory,
                                                  singletonList( profiler ),
                                                  jvm,
                                                  doFork,
                                                  jvmArgs,
                                                  resources );
                printForkInfo( forkName, query, profilerFork );
                runFork( query, unit, metricsPrinter, profilerFork );
            }

            // always run at least one fork. value 0 means run 1 in-process 'fork'
            for ( int forkNumber = 0; forkNumber < Math.max( measurementForkCount, 1 ); forkNumber++ )
            {
                String forkName = "measurement-fork-" + forkNumber;
                ForkDirectory forkDirectory = benchmarkDir.create( forkName, emptyList() );
                Path neo4jConfigFile = forkDirectory.create( "neo4j.conf" );
                Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );

                RunnableFork measurementFork = fork( launcher,
                                                     query,
                                                     store,
                                                     neo4jConfigFile,
                                                     forkDirectory,
                                                     new ArrayList<>(),
                                                     jvm,
                                                     doFork,
                                                     jvmArgs,
                                                     resources );
                // Export logical plan -- only necessary to do so once, every fork should produce the same plan
                // NOTE: this will use main process to create and export plans, if it becomes a problem an "export plan" command can be created
                if ( forkNumber == 0 )
                {
                    System.out.println( "Generating plan for : " + query.name() );
                    Path planFile = PlanCreator.exportPlan( forkDirectory, store, edition, neo4jConfigFile, query );
                    System.out.println( "Plan exported to    : " + planFile.toAbsolutePath().toString() );
                }
                printForkInfo( forkName, query, measurementFork );
                runFork( query, unit, metricsPrinter, measurementFork );
            }

            return benchmarkDir;
        }
        catch ( Exception exception )
        {
            throw new ForkFailureException( query, benchmarkDir, exception );
        }
    }

    private static void printForkInfo( String forkName, Query query, RunnableFork fork )
    {
        System.out.println( format( "Fork (%s): %s, Query: %s", fork.getClass().getSimpleName(), forkName, query.name() ) );
    }

    private static void runFork( Query query,
                                 TimeUnit unit,
                                 BenchmarkGroupBenchmarkMetricsPrinter metricsPrinter,
                                 RunnableFork fork )
    {
        Results results = fork.run().convertUnit( unit );
        BenchmarkGroupBenchmarkMetrics justForPrinting = new BenchmarkGroupBenchmarkMetrics();
        justForPrinting.add( query.benchmarkGroup(), query.benchmark(), results.metrics(), NO_NEO4J_CONFIG );
        System.out.println( metricsPrinter.toPrettyString( justForPrinting ) );
    }

    private static RunnableFork fork( DatabaseLauncher<?> launcher,
                                      Query query,
                                      Store store,
                                      Path neo4jConfigFile,
                                      ForkDirectory forkDirectory,
                                      List<ProfilerType> profilers,
                                      Jvm jvm,
                                      boolean doFork,
                                      List<String> jvmArgs,
                                      Resources resources )
    {
        return doFork
               ? new ForkingRunnable<>( launcher,
                                        query,
                                        forkDirectory,
                                        profilers,
                                        store,
                                        neo4jConfigFile,
                                        jvm,
                                        jvmArgs,
                                        resources )
               : new NonForkingRunnable<>( launcher,
                                           query,
                                           forkDirectory,
                                           profilers,
                                           store,
                                           neo4jConfigFile,
                                           jvm,
                                           jvmArgs,
                                           resources );
    }
}

