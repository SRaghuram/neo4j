/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.util.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.cli.ExportPlanCommand;
import com.neo4j.bench.macro.cli.RunMacroWorkloadCommand;
import com.neo4j.bench.macro.cli.RunSingleEmbeddedCommand;
import com.neo4j.bench.macro.cli.RunSingleServerCommand;
import com.neo4j.bench.macro.cli.ScheduleMacroCommand;
import com.neo4j.bench.macro.cli.UpgradeStoreCommand;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.execution.process.ForkFailureException;
import com.neo4j.bench.macro.execution.process.ForkRunner;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.process.JvmArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Main
{

    private static final Logger LOG = LoggerFactory.getLogger( Main.class );

    public static void main( List<String> args )
    {
        main( args.toArray( new String[]{} ) );
    }

    public static void main( String[] args )
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder( "bench" )
                .withDefaultCommand( Help.class )
                .withCommands(
                        RunMacroWorkloadCommand.class,
                        RunSingleEmbeddedCommand.class,
                        RunSingleServerCommand.class,
                        ExportPlanCommand.class,
                        UpgradeStoreCommand.class,
                        ScheduleMacroCommand.class,
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
            Query query = options.query();
            BenchmarkGroupDirectory benchmarkGroupDir = BenchmarkGroupDirectory.createAt( options.outputDir(), query.benchmarkGroup() );

            Neo4jDeployment neo4jDeployment = options.neo4jDeployment();
            BenchmarkDirectory benchmarkDir = ForkRunner.runForksFor( neo4jDeployment,
                                                                      benchmarkGroupDir,
                                                                      query,
                                                                      Neo4jStore.createFrom( options.storeDir() ),
                                                                      options.edition(),
                                                                      options.neo4jConfig(),
                                                                      options.profilers(),
                                                                      options.jvm(),
                                                                      options.forks(),
                                                                      options.unit(),
                                                                      verboseMetricsPrinter,
                                                                      JvmArgs.from( options.jvmArgs() ),
                                                                      resources );

            BenchmarkGroupBenchmarkMetrics queryResults = new BenchmarkGroupBenchmarkMetrics();
            Results results = Results.loadFrom( benchmarkDir );
            queryResults.add( query.benchmarkGroup(),
                              query.benchmark(),
                              results.metrics(),
                              results.rowMetrics(),
                              options.neo4jConfig() );

            LOG.debug( verboseMetricsPrinter.toPrettyString( queryResults ) );
        }
    }
}
