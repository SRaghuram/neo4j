/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.AddProfilesCommand;
import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.client.util.JsonUtil;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.client.util.TestSupport;
import com.neo4j.bench.macro.cli.RunWorkloadCommand;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.Options.ExecutionMode;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.Schema;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
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
                                   Neo4jDeployment.embedded(),
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
                                   Neo4jDeployment.embedded(),
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
                                   Neo4jDeployment.embedded(),
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
                                   Neo4jDeployment.server( getNeo4jDir() ),
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
                                   Neo4jDeployment.server( getNeo4jDir() ),
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
                                   Neo4jDeployment.server( getNeo4jDir() ),
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
                                   Neo4jDeployment.embedded(),
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
                                   Neo4jDeployment.embedded(),
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
                                   Neo4jDeployment.embedded(),
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
                                   Neo4jDeployment.server( getNeo4jDir() ),
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
                                   Neo4jDeployment.server( getNeo4jDir() ),
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
                                   Neo4jDeployment.server( getNeo4jDir() ),
                                   LOAD_CSV_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    private void executeWorkloadViaCommand( int measurementForks,
                                            Neo4jDeployment deployment,
                                            String workloadName,
                                            ArrayList<ProfilerType> profilers,
                                            int minimumExpectedProfilerRecordingCount ) throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.newFolder().toPath() ) )
        {
            Path outputDir = temporaryFolder.newFolder().toPath();
            Workload workload = Workload.fromName( workloadName, resources, deployment.mode() );
            Store store = createEmptyStoreFor( workload );

            Path neo4jConfiguration = temporaryFolder.newFile().toPath();
            Neo4jConfig.withDefaults().writeToFile( neo4jConfiguration );
            Path resultsJson = temporaryFolder.newFile().toPath();
            Path profilerRecordingsDir = outputDir.resolve( "profiler_recordings-" + workload.name() );
            Files.createDirectories( profilerRecordingsDir );
            boolean skipFlameGraphs = true;

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

            List<String> runWorkloadArgs = RunWorkloadCommand.argsFor(
                    Runtime.DEFAULT,
                    Planner.DEFAULT,
                    ExecutionMode.EXECUTE,
                    workload.name(),
                    store,
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
                    measurementForks,
                    minMeasurementDuration,
                    maxMeasurementDuration,
                    resultsJson,
                    TimeUnit.MICROSECONDS,
                    ErrorPolicy.FAIL,
                    parentTeamcityBuild,
                    teamcityBuild,
                    "-Xms4g -Xmx4g",
                    false,
                    profilerRecordingsDir,
                    skipFlameGraphs,
                    deployment,
                    triggeredBy );
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

    // Create empty store with valid schema, as expected by workload
    private Store createEmptyStoreFor( Workload workload ) throws IOException
    {
        Schema schema = workload.expectedSchema();
        Store store = TestSupport.createEmptyStore( temporaryFolder.newFolder().toPath() );
        Path neo4jConfigFile = temporaryFolder.newFile().toPath();
        EmbeddedDatabase.recreateSchema( store, Edition.ENTERPRISE, neo4jConfigFile, schema );
        return store;
    }

    private Path getNeo4jDir()
    {
        String neo4jDirString = System.getenv( "NEO4J_DIR" );
        return Paths.get( Objects.requireNonNull( neo4jDirString ) );
    }
}
