/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.client.results.BenchmarkDirectory;
import com.neo4j.bench.client.results.BenchmarkGroupDirectory;
import com.neo4j.bench.macro.cli.RunSingleCommand;
import com.neo4j.bench.macro.cli.RunWorkloadCommand;
import com.neo4j.bench.macro.cli.UpgradeStoreCommand;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.execution.process.ForkFailureException;
import com.neo4j.bench.macro.execution.process.ForkRunner;
import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Help;

public class Main
{
    public static void main( String[] args )
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder( "bench" )
                .withDefaultCommand( Help.class )
                .withCommands(
                        RunWorkloadCommand.class,
                        RunSingleCommand.class,
                        UpgradeStoreCommand.class,
                        Help.class );

        builder.build()
               .parse( args )
               .run();
    }

    public static void runInteractive( Options options ) throws ForkFailureException
    {
        // TODO maybe this should go via run workload command
        BenchmarkGroupBenchmarkMetricsPrinter verboseMetricsPrinter = new BenchmarkGroupBenchmarkMetricsPrinter( true );
        BenchmarkGroupDirectory benchmarkGroupDir = BenchmarkGroupDirectory.createAt( options.outputDir(), options.query().benchmarkGroup() );
        BenchmarkDirectory benchmarkDir = ForkRunner.runForksFor(
                benchmarkGroupDir,
                options.query(),
                Store.createFrom( options.storeDir() ),
                options.edition(),
                options.neo4jConfig(),
                options.profilers(),
                options.jvm(),
                options.forks(),
                options.warmupCount(),
                options.measurementCount(),
                options.unit(),
                verboseMetricsPrinter,
                options.jvmArgs() );

        BenchmarkGroupBenchmarkMetrics queryResults = new BenchmarkGroupBenchmarkMetrics();
        queryResults.add( options.query().benchmarkGroup(),
                          options.query().benchmark(),
                          Results.loadFrom( benchmarkDir ).metrics(),
                          options.neo4jConfig() );

        System.out.println( verboseMetricsPrinter.toPrettyString( queryResults ) );
    }
}
