/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.client.AddProfilesCommand;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.model.TestRunReport;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.cli.RunMacroWorkloadCommand;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@TestDirectoryExtension
class RunWorkloadCommandIT
{
    private static final String LOAD_CSV_WORKLOAD = "cineasts_csv";
    private static final String WRITE_WORKLOAD = "pokec_write";
    private static final String READ_WORKLOAD = "zero";

    @Inject
    private TestDirectory temporaryFolder;

    // <><><><><><><><><><><><> Forked - Embedded <><><><><><><><><><><><>

    @Test
    void executeReadWorkloadForkedWithEmbedded() throws Exception
    {
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 1,
                                   Deployment.embedded(),
                                   READ_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    @Disabled
    @Test
    void executeWriteWorkloadForkedWithEmbedded() throws Exception
    {
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 1,
                                   Deployment.embedded(),
                                   WRITE_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    @Disabled
    @Test
    void executeLoadCsvWorkloadForkedWithEmbedded() throws Exception
    {
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 1,
                                   Deployment.embedded(),
                                   LOAD_CSV_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    // <><><><><><><><><><><><> Forked - Server <><><><><><><><><><><><>

    // TODO uncomment when fixed
    //      writing path-value jvm args to neo4j conf causes failure on startup
    @Disabled
    @Test
    void executeReadWorkloadForkedWithServer() throws Exception
    {
        // TODO run with GC profiler too, once it works
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR );
        executeWorkloadViaCommand( 1,
                                   Deployment.server( getNeo4jDir() ),
                                   READ_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    @Disabled
    @Test
    void executeWriteWorkloadsForkedWithServer() throws Exception
    {
        // TODO run with GC profiler too, once it works
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR );
        executeWorkloadViaCommand( 1,
                                   Deployment.server( getNeo4jDir() ),
                                   WRITE_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    @Disabled
    @Test
    void executeLoadCsvWorkloadsForkedWithServer() throws Exception
    {
        // TODO run with GC profiler too, once it works
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR );
        executeWorkloadViaCommand( 1,
                                   Deployment.server( getNeo4jDir() ),
                                   LOAD_CSV_WORKLOAD,
                                   profilers,
                                   profilers.size() );
    }

    // <><><><><><><><><><><><> In-process - Embedded <><><><><><><><><><><><>

    @Test
    void executeReadWorkloadInProcessWithEmbedded() throws Exception
    {
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 0,
                                   Deployment.embedded(),
                                   READ_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    @Disabled
    @Test
    void executeWriteWorkloadInProcessWithEmbedded() throws Exception
    {
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 0,
                                   Deployment.embedded(),
                                   WRITE_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    @Disabled
    @Test
    void executeLoadCsvWorkloadInProcessWithEmbedded() throws Exception
    {
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR, ProfilerType.GC );
        executeWorkloadViaCommand( 0,
                                   Deployment.embedded(),
                                   LOAD_CSV_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    // <><><><><><><><><><><><> In-process - Server <><><><><><><><><><><><>

    // TODO uncomment when fixed
    //      writing path-value jvm args to neo4j conf causes failure on startup
    @Disabled
    @Test
    void executeReadWorkloadInProcessWithServer() throws Exception
    {
        // TODO run with GC profiler too, once it works
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR );
        executeWorkloadViaCommand( 0,
                                   Deployment.server( getNeo4jDir() ),
                                   READ_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    @Disabled
    @Test
    void executeWriteWorkloadInProcessWithServer() throws Exception
    {
        // TODO run with GC profiler too, once it works
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR );
        executeWorkloadViaCommand( 0,
                                   Deployment.server( getNeo4jDir() ),
                                   WRITE_WORKLOAD,
                                   profilers,
                                   // expect no recordings when running in-process, as both JFR & GC are external profilers
                                   0 );
    }

    @Disabled
    @Test
    void executeLoadCsvWorkloadInProcessWithServer() throws Exception
    {
        // TODO run with GC profiler too, once it works
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR );
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
                                            List<ParameterizedProfiler> profilers,
                                            int minimumExpectedProfilerRecordingCount ) throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
        {
            Path outputDir = Files.createTempDirectory( temporaryFolder.absolutePath().toPath(), "output" );
            Workload workload = Workload.fromName( workloadName, resources, deployment );
            Path neo4jConfigFile = Files.createTempFile( temporaryFolder.absolutePath().toPath(), "neo4j", ".conf" );
            Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfigFile );
            Store store = StoreTestUtil.createEmptyStoreFor( workload,
                                                             Files.createTempDirectory( temporaryFolder.absolutePath().toPath(), "store" ), // store
                                                             neo4jConfigFile );

            Path resultsJson = Files.createTempFile( temporaryFolder.absolutePath().toPath(), "neo4j", ".conf" );
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

            List<String> runWorkloadArgs = RunMacroWorkloadCommand.argsFor(
                    store.topLevelDirectory(),
                    neo4jConfigFile,
                    outputDir,
                    resultsJson,
                    profilerRecordingsDir,
                    new RunMacroWorkloadParams(
                            workload.name(),
                            emptyList(),
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
                            JvmArgs.from( "-Xms4g", "-Xmx4g" ),
                            recreateSchema,
                            skipFlameGraphs,
                            deployment,
                            neo4jCommit,
                            new Version( neo4jVersion ),
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

    private String getNeo4jDir()
    {
        return System.getenv( "NEO4J_DIR" );
    }
}
