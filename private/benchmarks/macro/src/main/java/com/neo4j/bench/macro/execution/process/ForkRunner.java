package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.BenchmarkDirectory;
import com.neo4j.bench.client.results.BenchmarkGroupDirectory;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
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

    public static BenchmarkDirectory runForksFor( BenchmarkGroupDirectory groupDir,
                                                  Query query,
                                                  Store store,
                                                  Edition edition,
                                                  Neo4jConfig neo4jConfig,
                                                  List<ProfilerType> profilers,
                                                  Jvm jvm,
                                                  int measurementForkCount,
                                                  int warmupCount,
                                                  int measurementCount,
                                                  TimeUnit unit,
                                                  BenchmarkGroupBenchmarkMetricsPrinter metricsPrinter,
                                                  List<String> jvmArgs ) throws ForkFailureException
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
                neo4jConfig.writeAsProperties( neo4jConfigFile );
                RunnableFork profilerFork = fork( query,
                                                  store,
                                                  edition,
                                                  neo4jConfigFile,
                                                  forkDirectory,
                                                  singletonList( profiler ),
                                                  jvm,
                                                  doFork,
                                                  warmupCount,
                                                  measurementCount,
                                                  jvmArgs,
                                                  // no need to export logical plan in profiling runs
                                                  false );

                System.out.println( format( "Fork (%s): %s", profilerFork.getClass().getSimpleName(), forkName ) );
                // retrieve results from profiling forks, but only for the purpose of displaying them
                Results results = profilerFork.run().convertUnit( unit );
                BenchmarkGroupBenchmarkMetrics justForPrinting = new BenchmarkGroupBenchmarkMetrics();
                justForPrinting.add( query.benchmarkGroup(), query.benchmark(), results.metrics(), NO_NEO4J_CONFIG );
                System.out.println( metricsPrinter.toPrettyString( justForPrinting ) );
            }

            // always run at least one fork. value 0 means run 1 in-process 'fork'
            for ( int forkNumber = 0; forkNumber < Math.max( measurementForkCount, 1 ); forkNumber++ )
            {
                String forkName = "measurement-fork-" + forkNumber;
                ForkDirectory forkDirectory = benchmarkDir.create( forkName, emptyList() );
                Path neo4jConfigFile = forkDirectory.create( "neo4j.conf" );
                neo4jConfig.writeAsProperties( neo4jConfigFile );
                RunnableFork measurementFork = fork( query,
                                                     store,
                                                     edition,
                                                     neo4jConfigFile,
                                                     forkDirectory,
                                                     new ArrayList<>(),
                                                     jvm,
                                                     doFork,
                                                     warmupCount,
                                                     measurementCount,
                                                     jvmArgs,
                                                     // only necessary to export logical plans from one measurement fork
                                                     forkNumber == 0 );

                System.out.println( format( "Fork (%s): %s", measurementFork.getClass().getSimpleName(), forkName ) );
                Results forkResults = measurementFork.run().convertUnit( unit );
                BenchmarkGroupBenchmarkMetrics justForPrinting = new BenchmarkGroupBenchmarkMetrics();
                justForPrinting.add( query.benchmarkGroup(), query.benchmark(), forkResults.metrics(), NO_NEO4J_CONFIG );
                System.out.println( metricsPrinter.toPrettyString( justForPrinting ) );
            }

            return benchmarkDir;
        }
        catch ( Exception exception )
        {
            throw new ForkFailureException( query, benchmarkDir, exception );
        }
    }

    private static RunnableFork fork( Query query,
                                      Store store,
                                      Edition edition,
                                      Path neo4jConfigFile,
                                      ForkDirectory forkDirectory,
                                      List<ProfilerType> profilers,
                                      Jvm jvm,
                                      boolean doFork,
                                      int warmupCount,
                                      int measurementCount,
                                      List<String> jvmArgs,
                                      boolean doExportPlan )
    {
        return doFork
               ? new ForkingRunnable( query,
                                      forkDirectory,
                                      profilers,
                                      store,
                                      edition,
                                      neo4jConfigFile,
                                      jvm,
                                      warmupCount,
                                      measurementCount,
                                      jvmArgs,
                                      doExportPlan )
               : new NonForkingRunnable( query,
                                         forkDirectory,
                                         profilers,
                                         store,
                                         edition,
                                         neo4jConfigFile,
                                         warmupCount,
                                         measurementCount,
                                         doExportPlan,
                                         jvm );
    }
}

