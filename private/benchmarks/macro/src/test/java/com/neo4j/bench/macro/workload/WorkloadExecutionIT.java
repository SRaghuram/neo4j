/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.BenchmarkDirectory;
import com.neo4j.bench.client.results.BenchmarkGroupDirectory;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.macro.Main;
import com.neo4j.bench.macro.TestSupport;
import com.neo4j.bench.macro.cli.RunWorkloadCommand;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.Options.ExecutionMode;
import com.neo4j.bench.macro.execution.OptionsBuilder;
import com.neo4j.bench.macro.execution.QueryRunner;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.execution.process.ForkFailureException;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.ofCount;
import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.ofDuration;
import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.or;
import static java.time.Duration.ofSeconds;

public class WorkloadExecutionIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    // TODO make it possible to run against non-standard workloads somehow
    @Ignore
    @Test
    public void executeTestWorkloadInForks() throws Exception
    {
        try ( Resources resources = new Resources() )
        {
            int measurementForkCount = 3;
            ArrayList<ProfilerType> profilers = Lists.newArrayList();
            Path outputDir = temporaryFolder.newFolder().toPath();
            Path workloadConfigFile = resources.resourceFile( "/test_workloads/test/integration_test.json" );
            Workload workload = Workload.fromFile( workloadConfigFile );
            Store emptyStore = TestSupport.createEmptyStore( temporaryFolder.newFolder().toPath() );
            runEveryQueryForWorkloadUsingForkingRunner( measurementForkCount,
                                                        profilers,
                                                        ExecutionMode.EXECUTE,
                                                        outputDir,
                                                        ErrorPolicy.FAIL,
                                                        workload,
                                                        emptyStore );
        }
    }

    // TODO make it possible to run against non-standard workloads somehow
    @Ignore
    @Test
    public void executeTestWorkloadInProcess() throws Exception
    {
        try ( Resources resources = new Resources() )
        {
            int measurementForkCount = 0;
            ArrayList<ProfilerType> profilers = Lists.newArrayList();
            Path outputDir = temporaryFolder.newFolder().toPath();
//            Workload workload = Workload.fromName( "test", resources );
            Path workloadConfigFile = resources.resourceFile( "/test_workloads/test/integration_test.json" );
            Workload workload = Workload.fromFile( workloadConfigFile );
            Store emptyStore = TestSupport.createEmptyStore( temporaryFolder.newFolder().toPath() );
            runEveryQueryForWorkloadUsingForkingRunner( measurementForkCount,
                                                        profilers,
                                                        ExecutionMode.EXECUTE,
                                                        outputDir,
                                                        ErrorPolicy.FAIL,
                                                        workload,
                                                        emptyStore );
        }
    }

    @Test
    public void executeTestWorkloadUsingEmbeddedRunner() throws Exception
    {
        try ( Resources resources = new Resources() )
        {
            Path workloadConfigFile = resources.resourceFile( "/test_workloads/test/integration_test.json" );
            Workload workload = Workload.fromFile( workloadConfigFile );
            Path storeDir = temporaryFolder.newFolder().toPath();
            runEveryQueryFromWorkloadUsingEmbeddedRunner( workload, storeDir );
        }
    }

    // TODO make it possible to run against non-standard workloads somehow
    @Ignore
    @Test
    public void executeTestWorkloadUsingInteractiveMode() throws Exception
    {
        try ( Resources resources = new Resources() )
        {
            Path workloadConfigFile = resources.resourceFile( "/test_workloads/test/integration_test.json" );
            Workload workload = Workload.fromFile( workloadConfigFile );
            Path storeDir = temporaryFolder.newFolder().toPath();
            runEveryQueryFromWorkloadUsingInteractiveMode( workload, storeDir );
        }
    }

    private void runEveryQueryFromWorkloadUsingEmbeddedRunner( Workload workload,
                                                               Path storeDir ) throws Exception
    {
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( temporaryFolder.newFolder().toPath(),
                                                                             workload.benchmarkGroup() );

        MeasurementControl warmupControl = or( ofCount( 10 ), ofDuration( ofSeconds( 10 ) ) );
        MeasurementControl measurementControl = or( ofCount( 10 ), ofDuration( ofSeconds( 10 ) ) );

        QueryRunner queryRunner = QueryRunner.runnerFor( ExecutionMode.EXECUTE );
        for ( Query query : workload.queries() )
        {
            BenchmarkDirectory benchmarkDir = groupDir.findOrCreate( query.benchmark() );
            ForkDirectory forkDir = benchmarkDir.create( "fork_name", new ArrayList<>() );

            Path neo4jConfigFile = temporaryFolder.newFile().toPath();
            Neo4jConfig neo4jConfig = Neo4jConfig.fromFile( neo4jConfigFile );

            Store store = TestSupport.createEmptyStore( storeDir );
            queryRunner.run(
                    Jvm.defaultJvmOrFail(),
                    store,
                    Edition.ENTERPRISE,
                    neo4jConfig,
                    Lists.newArrayList(), // profilers
                    query,
                    forkDir,
                    warmupControl,
                    measurementControl );
        }
    }

    private void runEveryQueryFromWorkloadUsingInteractiveMode( Workload workload,
                                                                Path storeDir ) throws IOException, ForkFailureException
    {
        // STORES_DIR.resolve( workload.name() + "/graph.db" )
        OptionsBuilder optionsBuilder = new OptionsBuilder()
                .withForks( 0 )
                .withWarmupCount( 1 )
                .withMeasurementCount( 1 )
                .withPrintResults( true )
                .withUnit( TimeUnit.MICROSECONDS );

        for ( Query query : workload.queries() )
        {
            Path outputDir = temporaryFolder.newFolder().toPath();
            Options options = optionsBuilder
                    .withOutputDir( outputDir )
                    .withStoreDir( storeDir )
                    .withQuery( query )
                    .build();
            Main.runInteractive( options );
        }
    }

    private void runEveryQueryForWorkloadUsingForkingRunner( int measurementForkCount,
                                                             ArrayList<ProfilerType> profilers,
                                                             ExecutionMode executionMode,
                                                             Path outputDir,
                                                             ErrorPolicy errorPolicy,
                                                             Workload workload,
                                                             Store storeDir ) throws Exception
    {
        Path neo4jConfiguration = temporaryFolder.newFile().toPath();
        Path resultsJson = temporaryFolder.newFile().toPath();
        Path profilerRecordingsDir = outputDir.resolve( "profiler_recordings-" + workload.name() );
        boolean skipFlameGraphs = true;

        String neo4jVersion = "1.2.3";
        String neo4jBranch = "1.2";
        String neo4jBranchOwner = "neo-technology";
        String neo4jCommit = "abcd123";

        String toolBranch = "0.1";
        String toolBranchOwner = "neo-technology";
        String toolCommit = "1234abc";

        long parentTeamcityBuild = 0;
        long teamcityBuild = 1;

        int warmupCount = 100;
        int measurementCount = 100;

        List<String> runWorkloadArgs = RunWorkloadCommand.argsFor(
                Runtime.DEFAULT,
                Planner.DEFAULT,
                executionMode,
                workload.name(),
                storeDir,
                neo4jConfiguration,
                neo4jVersion,
                neo4jBranch,
                neo4jCommit,
                neo4jBranchOwner,
                toolBranch,
                toolCommit,
                toolBranchOwner,
                outputDir,
                profilers,
                Edition.ENTERPRISE,
                Jvm.defaultJvm(),
                warmupCount,
                measurementCount,
                measurementForkCount,
                resultsJson,
                TimeUnit.MICROSECONDS,
                errorPolicy,
                parentTeamcityBuild,
                teamcityBuild,
                "-Xms4g -Xmx4g",
                false,
                profilerRecordingsDir,
                skipFlameGraphs );
        Main.main( runWorkloadArgs.stream().toArray( String[]::new ) );
    }

    static String databaseNameFor( Workload workload )
    {
        if ( workload.name().startsWith( "generatedmusicdata" ) )
        {
            return "generatedmusicdata";
        }
        else if ( workload.name().startsWith( "qmul" ) )
        {
            return "qmul";
        }
        else if ( workload.name().startsWith( "pokec" ) )
        {
            return "pokec";
        }
        else if ( workload.name().equalsIgnoreCase( "index_backed_order_by" ) )
        {
            return "pokec";
        }
        else
        {
            return workload.name();
        }
    }
}
