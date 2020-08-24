/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.process.JpsPid;
import com.neo4j.bench.common.process.JvmProcess;
import com.neo4j.bench.common.process.JvmProcessArgs;
import com.neo4j.bench.common.process.PgrepAndPsPid;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.Main;
import com.neo4j.bench.macro.cli.ExportPlanCommand;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

public class ForkRunner
{

    private static final Logger LOG = LoggerFactory.getLogger( ForkRunner.class );

    private static final Neo4jConfig NO_NEO4J_CONFIG = Neo4jConfig.empty();

    public static BenchmarkDirectory runForksFor( DatabaseLauncher<?> launcher,
                                                  BenchmarkGroupDirectory groupDir,
                                                  Query query,
                                                  Store store,
                                                  Edition edition,
                                                  Neo4jConfig neo4jConfig,
                                                  List<ParameterizedProfiler> profilers,
                                                  Jvm jvm,
                                                  int measurementForkCount,
                                                  TimeUnit unit,
                                                  BenchmarkGroupBenchmarkMetricsPrinter metricsPrinter,
                                                  JvmArgs jvmArgs,
                                                  Resources resources ) throws ForkFailureException
    {
        BenchmarkDirectory benchmarkDir = groupDir.findOrCreate( query.benchmark() );
        boolean doFork = measurementForkCount != 0;
        try
        {
            // profiler forks: each profiler run in separate fork
            for ( ParameterizedProfiler profiler : profilers )
            {
                String forkName = "profiler-fork-" + profiler.profilerType().name().toLowerCase();
                ForkDirectory forkDirectory = benchmarkDir.create( forkName );
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
                LOG.debug( profilerFork.toString() );
                runFork( query, unit, metricsPrinter, profilerFork );
            }

            // always run at least one fork. value 0 means run 1 in-process 'fork'
            for ( int forkNumber = 0; forkNumber < Math.max( measurementForkCount, 1 ); forkNumber++ )
            {
                String forkName = "measurement-fork-" + forkNumber;
                ForkDirectory forkDirectory = benchmarkDir.create( forkName );
                Path neo4jConfigFile = forkDirectory.create( "neo4j.conf" );
                Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );

                RunnableFork measurementFork = fork( launcher,
                                                     query,
                                                     store,
                                                     neo4jConfigFile,
                                                     forkDirectory,
                                                     ParameterizedProfiler.defaultProfilers( ProfilerType.OOM ),
                                                     jvm,
                                                     doFork,
                                                     jvmArgs,
                                                     resources );
                // Export logical plan -- only necessary to do so once, every fork should produce the same plan
                if ( forkNumber == 0 )
                {
                    runPlanExportFork( query,
                                       store,
                                       edition,
                                       neo4jConfigFile,
                                       forkDirectory,
                                       resources,
                                       jvm,
                                       jvmArgs,
                                       doFork );
                }
                LOG.debug( measurementFork.toString() );
                runFork( query, unit, metricsPrinter, measurementFork );
            }

            return benchmarkDir;
        }
        catch ( Exception exception )
        {
            throw new ForkFailureException( query, benchmarkDir, exception );
        }
    }

    private static void runFork( Query query,
                                 TimeUnit unit,
                                 BenchmarkGroupBenchmarkMetricsPrinter metricsPrinter,
                                 RunnableFork fork )
    {
        Results results = fork.run().convertUnit( unit );
        BenchmarkGroupBenchmarkMetrics justForPrinting = new BenchmarkGroupBenchmarkMetrics();
        justForPrinting.add( query.benchmarkGroup(), query.benchmark(), results.metrics(), results.rowMetrics(), NO_NEO4J_CONFIG );
        LOG.debug( metricsPrinter.toPrettyString( justForPrinting ) );
    }

    private static RunnableFork fork( DatabaseLauncher<?> launcher,
                                      Query query,
                                      Store store,
                                      Path neo4jConfigFile,
                                      ForkDirectory forkDirectory,
                                      List<ParameterizedProfiler> profilers,
                                      Jvm jvm,
                                      boolean doFork,
                                      JvmArgs jvmArgs,
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

    private static void runPlanExportFork( Query query,
                                           Store originalStore,
                                           Edition edition,
                                           Path neo4jConfigFile,
                                           ForkDirectory forkDirectory,
                                           Resources resources,
                                           Jvm jvm,
                                           JvmArgs jvmArgs,
                                           boolean doFork )
    {
        try ( Store store = (query.isMutating())
                            ? originalStore.makeTemporaryCopy()
                            : originalStore )
        {
            List<String> commandArgs = ExportPlanCommand.argsFor( query, store, edition, neo4jConfigFile, forkDirectory, resources.workDir() );

            if ( doFork )
            {
                JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( Collections.emptyList(),
                                                                                  jvm,
                                                                                  jvmArgs,
                                                                                  commandArgs,
                                                                                  Main.class );
                // inherit output
                ProcessBuilder.Redirect outputRedirect = ProcessBuilder.Redirect.INHERIT;
                // redirect error to file
                ProcessBuilder.Redirect errorRedirect = ProcessBuilder.Redirect.to( forkDirectory.newErrorLog().toFile() );
                JvmProcess.start( jvmProcessArgs,
                                  outputRedirect,
                                  errorRedirect,
                                  Arrays.asList( new JpsPid(), new PgrepAndPsPid() ) ).waitFor();
            }
            else
            {
                Main.main( commandArgs.toArray( new String[0] ) );
            }
        }
    }
}

