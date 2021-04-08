/*
 * Copyright (c) "Neo4j"
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
import com.neo4j.bench.macro.Main;
import com.neo4j.bench.macro.cli.ExportPlanCommand;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

public class ForkRunner
{

    private static final Logger LOG = LoggerFactory.getLogger( ForkRunner.class );

    private static final Neo4jConfig NO_NEO4J_CONFIG = Neo4jConfig.empty();

    public static Results runForksFor( Neo4jDeployment neo4jDeployment,
                                       BenchmarkGroupDirectory groupDir,
                                       Query query,
                                       Edition edition,
                                       Neo4jConfig neo4jConfig,
                                       List<ParameterizedProfiler> profilers,
                                       Jvm jvm,
                                       int measurementForkCount,
                                       TimeUnit unit,
                                       BenchmarkGroupBenchmarkMetricsPrinter metricsPrinter,
                                       JvmArgs baseJvmArgs,
                                       Path workDir ) throws ForkFailureException
    {
        BenchmarkDirectory benchmarkDir = benchmarkDirFor( groupDir, query );
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
                RunnableFork profilerFork = fork( neo4jDeployment,
                                                  query,
                                                  forkDirectory,
                                                  singletonList( profiler ),
                                                  jvm,
                                                  doFork,
                                                  baseJvmArgs,
                                                  neo4jConfigFile );
                LOG.debug( profilerFork.toString() );
                runFork( query, unit, metricsPrinter, profilerFork );
            }

            List<ForkDirectory> measurementForks = new ArrayList<>();
            // always run at least one fork. value 0 means run 1 in-process 'fork'
            for ( int forkNumber = 0; forkNumber < Math.max( measurementForkCount, 1 ); forkNumber++ )
            {
                String forkName = "measurement-fork-" + forkNumber;
                ForkDirectory forkDirectory = benchmarkDir.create( forkName );
                Path neo4jConfigFile = forkDirectory.create( "neo4j.conf" );
                Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );
                RunnableFork measurementFork = fork( neo4jDeployment,
                                                     query,
                                                     forkDirectory,
                                                     ParameterizedProfiler.defaultProfilers( ProfilerType.OOM ),
                                                     jvm,
                                                     doFork,
                                                     baseJvmArgs,
                                                     neo4jConfigFile );
                // Export logical plan -- only necessary to do so once, every fork should produce the same plan
                if ( forkNumber == 0 )
                {
                    runPlanExportFork( query,
                                       neo4jDeployment.originalStore(),
                                       edition,
                                       neo4jConfigFile,
                                       forkDirectory,
                                       workDir,
                                       jvm,
                                       baseJvmArgs,
                                       doFork );
                }
                LOG.debug( measurementFork.toString() );
                runFork( query, unit, metricsPrinter, measurementFork );
                measurementForks.add( forkDirectory );
            }

            return Results.loadFrom( measurementForks );
        }
        catch ( Exception exception )
        {
            throw new ForkFailureException( query, benchmarkDir, exception );
        }
    }

    public static BenchmarkDirectory benchmarkDirFor( BenchmarkGroupDirectory groupDir, Query query )
    {
        return groupDir.findOrCreate( query.benchmark() );
    }

    private static RunnableFork fork( Neo4jDeployment neo4jDeployment,
                                      Query query,
                                      ForkDirectory forkDirectory,
                                      List<ParameterizedProfiler> parameterizedProfilers,
                                      Jvm jvm,
                                      boolean doFork,
                                      JvmArgs baseJvmArgs,
                                      Path neo4jConfigFile )
    {
        DatabaseLauncher launcher = neo4jDeployment.launcherFor( query,
                                                                 neo4jConfigFile,
                                                                 forkDirectory,
                                                                 baseJvmArgs,
                                                                 parameterizedProfilers );
        return doFork
               ? new ForkingRunnable<>( launcher, forkDirectory, jvm )
               : new NonForkingRunnable<>( launcher, forkDirectory );
    }

    private static void runFork( Query query,
                                 TimeUnit unit,
                                 BenchmarkGroupBenchmarkMetricsPrinter metricsPrinter,
                                 RunnableFork fork )
    {
        Results rawResults = fork.run();
        Results results = (query.benchmark().mode().equals( Benchmark.Mode.ACCURACY ))
                          ? rawResults
                          : rawResults.convertTimeUnit( unit );
        BenchmarkGroupBenchmarkMetrics justForPrinting = new BenchmarkGroupBenchmarkMetrics();
        justForPrinting.add( query.benchmarkGroup(), query.benchmark(), results.metrics(), results.rowMetrics(), NO_NEO4J_CONFIG );
        LOG.debug( metricsPrinter.toPrettyString( justForPrinting ) );
    }

    private static void runPlanExportFork( Query query,
                                           Store originalStore,
                                           Edition edition,
                                           Path neo4jConfigFile,
                                           ForkDirectory forkDirectory,
                                           Path workDir,
                                           Jvm jvm,
                                           JvmArgs jvmArgs,
                                           boolean doFork )
    {
        try ( Store store = (query.isMutating())
                            ? originalStore.makeTemporaryCopy()
                            : originalStore )
        {
            List<String> commandArgs = ExportPlanCommand.argsFor( query, store, edition, neo4jConfigFile, forkDirectory, workDir );

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

