/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.util.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.cli.RunSingleEmbeddedCommand;
import com.neo4j.bench.macro.cli.RunSingleServerCommand;
import com.neo4j.bench.macro.cli.RunWorkloadCommand;
import com.neo4j.bench.macro.cli.UpgradeStoreCommand;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.execution.process.ForkFailureException;
import com.neo4j.bench.macro.execution.process.ForkRunner;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Main
{
    public static void main( String[] args )
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder( "bench" )
                .withDefaultCommand( Help.class )
                .withCommands(
                        RunWorkloadCommand.class,
                        RunSingleEmbeddedCommand.class,
                        RunSingleServerCommand.class,
                        UpgradeStoreCommand.class,
                        Help.class );

        builder.build()
               .parse( args )
               .run();
    }

    static void runInteractive( Options options ) throws ForkFailureException
    {
        Path workDir = Paths.get( System.getProperty( "user.dir" ) );
        try ( Resources resources = new Resources( workDir ) )
        {
            BenchmarkGroupBenchmarkMetricsPrinter verboseMetricsPrinter = new BenchmarkGroupBenchmarkMetricsPrinter( true );
            BenchmarkGroupDirectory benchmarkGroupDir = BenchmarkGroupDirectory.createAt( options.outputDir(), options.query().benchmarkGroup() );

            BenchmarkDirectory benchmarkDir = ForkRunner.runForksFor(
                    options.neo4jDeployment().launcherFor( Edition.ENTERPRISE,
                                                           options.warmupCount(),
                                                           options.measurementCount(),
                                                           options.minDuration(),
                                                           options.maxDuration(),
                                                           options.jvm() ),
                    benchmarkGroupDir,
                    options.query(),
                    Store.createFrom( options.storeDir() ),
                    options.edition(),
                    options.neo4jConfig(),
                    options.profilers(),
                    options.jvm(),
                    options.forks(),
                    options.unit(),
                    verboseMetricsPrinter,
                    options.jvmArgs(),
                    resources );

            BenchmarkGroupBenchmarkMetrics queryResults = new BenchmarkGroupBenchmarkMetrics();
            queryResults.add( options.query().benchmarkGroup(),
                              options.query().benchmark(),
                              Results.loadFrom( benchmarkDir ).metrics(),
                              options.neo4jConfig() );

            System.out.println( verboseMetricsPrinter.toPrettyString( queryResults ) );
        }
    }
}
