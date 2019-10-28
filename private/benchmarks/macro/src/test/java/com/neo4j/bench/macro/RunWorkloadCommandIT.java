/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.AddProfilesCommand;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.model.TestRunReport;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.common.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.cli.RunWorkloadCommand;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class RunWorkloadCommandIT
{
    private static final String LOAD_CSV_WORKLOAD = "cineasts_csv";
    private static final String WRITE_WORKLOAD = "pokec_write";
    private static final String READ_WORKLOAD = "zero";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    // <><><><><><><><><><><><> Forked - Embedded <><><><><><><><><><><><>

    @Test
    public void executeReadWorkloadForkedWithEmbedded() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 1,
                                   Deployment.embedded(),
                                   READ_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    @Ignore
    @Test
    public void executeWriteWorkloadForkedWithEmbedded() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 1,
                                   Deployment.embedded(),
                                   WRITE_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    @Ignore
    @Test
    public void executeLoadCsvWorkloadForkedWithEmbedded() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 1,
                                   Deployment.embedded(),
                                   LOAD_CSV_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    // <><><><><><><><><><><><> Forked - Server <><><><><><><><><><><><>

    @Test
    public void executeReadWorkloadForkedWithServer() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 1,
                                   Deployment.server( getNeo4jDir() ),
                                   READ_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    @Ignore
    @Test
    public void executeWriteWorkloadsForkedWithServer() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 1,
                                   Deployment.server( getNeo4jDir() ),
                                   WRITE_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    @Ignore
    @Test
    public void executeLoadCsvWorkloadsForkedWithServer() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 1,
                                   Deployment.server( getNeo4jDir() ),
                                   LOAD_CSV_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    // <><><><><><><><><><><><> In-process - Embedded <><><><><><><><><><><><>

    @Test
    public void executeReadWorkloadInProcessWithEmbedded() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 0,
                                   Deployment.embedded(),
                                   READ_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    @Ignore
    @Test
    public void executeWriteWorkloadInProcessWithEmbedded() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 0,
                                   Deployment.embedded(),
                                   WRITE_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    @Ignore
    @Test
    public void executeLoadCsvWorkloadInProcessWithEmbedded() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 0,
                                   Deployment.embedded(),
                                   LOAD_CSV_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    // <><><><><><><><><><><><> In-process - Server <><><><><><><><><><><><>

    @Test
    public void executeReadWorkloadInProcessWithServer() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 0,
                                   Deployment.server( getNeo4jDir() ),
                                   READ_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    @Ignore
    @Test
    public void executeWriteWorkloadInProcessWithServer() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 0,
                                   Deployment.server( getNeo4jDir() ),
                                   WRITE_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    @Ignore
    @Test
    public void executeLoadCsvWorkloadInProcessWithServer() throws Exception
    {
        ArrayList<ProfilerType> profilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 0,
                                   Deployment.server( getNeo4jDir() ),
                                   LOAD_CSV_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    private void executeWorkloadViaCommand( int measurementForks,
                                            Deployment deployment,
                                            String workloadName,
                                            ArrayList<ProfilerType> profilers,
                                            int minimumExpectedProfilerRecordingCount ) throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.newFolder().toPath() ) )
        {
            Path outputDir = temporaryFolder.newFolder().toPath();
            Workload workload = Workload.fromName( workloadName, resources, deployment );
            Store store = StoreTestUtil.createEmptyStoreFor( workload,
                                                             temporaryFolder.newFolder().toPath(), // store
                                                             temporaryFolder.newFile().toPath() ); // neo4j config

            Path neo4jConfigFile = temporaryFolder.newFile().toPath();
            Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfigFile );

            Path resultsJson = outputDir.resolve( "results.json" );
            Path profilerRecordingsDir = outputDir.resolve( "profiler_recordings-" + workload.name() );
            Files.createDirectories( profilerRecordingsDir );
            boolean skipFlameGraphs = true;
            boolean recreateSchema = false;

            String neo4jVersion = "1.2.3";
            String neo4jBranch = "1.2";
            String neo4jBranchOwner = "neo-technology";
            String neo4jCommit = "abcd123";

            String toolBranch = "0.1";
            String toolBranchOwner = "neo-technology";
            String toolCommit = "1234abc";

            String triggeredBy = "xyz";

            long parentTeamcityBuild = 0;
            long teamcityBuild = 1;

            int warmupCount = 2;
            int measurementCount = 2;
            Duration minMeasurementDuration = Duration.ofSeconds( 10 );
            Duration maxMeasurementDuration = Duration.ofSeconds( 10 );

            Path jvmPath = Paths.get( Jvm.defaultJvmOrFail().launchJava() );

            List<String> runWorkloadArgs = RunWorkloadCommand.argsFor(
                    store.topLevelDirectory(),
                    neo4jConfigFile,
                    outputDir,
                    resultsJson,
                    profilerRecordingsDir,
                    new RunWorkloadParams(
                            workload.name(),
                            Edition.ENTERPRISE,
                            jvmPath,
                            profilers,
                            warmupCount,
                            measurementCount,
                            minMeasurementDuration,
                            maxMeasurementDuration,
                            measurementForks,
                            TimeUnit.MICROSECONDS,
                            Runtime.DEFAULT,
                            Planner.DEFAULT,
                            ExecutionMode.EXECUTE,
                            ErrorPolicy.FAIL,
                            JvmArgs.from( "-Xms4g", "-Xmx4g" ),
                            recreateSchema,
                            skipFlameGraphs,
                            deployment,
                            neo4jCommit,
                            neo4jVersion,
                            neo4jBranch,
                            neo4jBranchOwner,
                            toolCommit,
                            toolBranchOwner,
                            toolBranch,
                            teamcityBuild,
                            parentTeamcityBuild,
                            triggeredBy ) );

            Main.main( runWorkloadArgs.stream().toArray( String[]::new ) );

            // should find at least one recording per profiler per benchmark -- there may be more, due to secondary recordings
            int profilerRecordingCount = (int) Files.list( profilerRecordingsDir ).count();
            assertThat( profilerRecordingCount, greaterThanOrEqualTo( minimumExpectedProfilerRecordingCount ) );

            // it should be possible to load all the created profiler recordings
            boolean ignoreUnrecognizedFiles = false;
            List<String> addProfilesArgs = AddProfilesCommand.argsFor( profilerRecordingsDir,
                                                                       resultsJson,
                                                                       "s3-bucket-name",
                                                                       "s3-bucket-name/some_archive.tgz",
                                                                       ignoreUnrecognizedFiles );
            com.neo4j.bench.client.Main.main( addProfilesArgs.stream().toArray( String[]::new ) );

            // should have attached at least one recording per profiler per benchmark
            TestRunReport testRunReport = JsonUtil.deserializeJson( resultsJson, TestRunReport.class );
            for ( Query query : workload.queries() )
            {
                int attachedProfilerRecordingsCount = testRunReport.benchmarkGroupBenchmarkMetrics()
                                                                   .getMetricsFor( query.benchmarkGroup(), query.benchmark() )
                                                                   .profilerRecordings()
                                                                   .toMap()
                                                                   .size();
                assertThat( attachedProfilerRecordingsCount, greaterThanOrEqualTo( minimumExpectedProfilerRecordingCount ) );
            }
        }
    }

    private Path getNeo4jDir()
    {
        String neo4jDirString = System.getenv( "NEO4J_DIR" );
        return Paths.get( Objects.requireNonNull( neo4jDirString ) );
    }
}
