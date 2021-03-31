/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.tool.macro.BuildParams;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.MeasurementParams;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.util.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.cli.RunMacroWorkloadCommand;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.process.ForkRunner;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.configuration.SettingValueParsers.FALSE;

@TestDirectoryExtension
class ConvenientLocalExecutionIT
{
    // Required fields for running whole Workload or Single query
    private static final Path STORE_DIR = null; // e.g. /Users/you/stores/3.5/ldbc_sf001_data/ not /Users/you/stores/3.5/ldbc_sf001_data/graph.db/
    private static final Path RESULT_DIR = null; // e.g. /Users/you/results/
    private static final String WORKLOAD_NAME = null; // e.g. "ldbc_sf001"
    private static final Deployment DEPLOYMENT = Deployment.embedded();

    // Optional fields
    private static final boolean SKIP_FLAME_GRAPHS = false;
    private static final Path JDK_DIR = null;
    private static final Path NEO4J_CONFIG = null;
    private static final int FORK_COUNT = 1;
    private static final int WARMUP_COUNT = 1;
    private static final int MEASUREMENT_COUNT = 1;
    private static final MeasurementParams MEASUREMENT_PARAMS =
            new MeasurementParams( WARMUP_COUNT, MEASUREMENT_COUNT, Duration.ofSeconds( 0 ), Duration.ofSeconds( 10 ) );
    private static final List<ParameterizedProfiler> PROFILERS = ParameterizedProfiler.defaultProfilers( ProfilerType.JFR );
    private static final ExecutionMode EXECUTION_MODE = ExecutionMode.EXECUTE;
    private static final JvmArgs JVM_ARGS = JvmArgs.from( "-Xms4g", "-Xmx4g" );
    private static final boolean RECREATE_SCHEMA = false;
    private static final Edition EDITION = Edition.ENTERPRISE;
    private static final Planner PLANNER = Planner.DEFAULT;
    private static final Runtime RUNTIME = Runtime.DEFAULT;

    @Inject
    private TestDirectory temporaryFolder;

    @Disabled
    @Test
    void executeWorkload() throws Exception
    {
        Version neo4jVersion = new Version( "1.2.3" );
        String neo4jBranch = "1.2";
        String neo4jBranchOwner = "neo-technology";
        String neo4jCommit = "abcd123";
        long parentTeamcityBuild = 0;
        long teamcityBuild = 1;
        String triggeredBy = "xyz";

        Path profilerRecordingsDir = RESULT_DIR.resolve( "profiler_recordings-" + WORKLOAD_NAME );
        Files.createDirectories( profilerRecordingsDir );
        Path jvmPath = Paths.get( Jvm.defaultJvmOrFail().launchJava() );

        RunMacroWorkloadParams workloadParams = new RunMacroWorkloadParams(
                WORKLOAD_NAME,
                emptyList(),
                EDITION,
                jvmPath,
                PROFILERS,
                MEASUREMENT_PARAMS,
                FORK_COUNT,
                TimeUnit.MICROSECONDS,
                RUNTIME,
                PLANNER,
                EXECUTION_MODE,
                JVM_ARGS,
                RECREATE_SCHEMA,
                SKIP_FLAME_GRAPHS,
                DEPLOYMENT,
                new BuildParams( neo4jCommit,
                                 neo4jVersion,
                                 neo4jBranch,
                                 neo4jBranchOwner,
                                 teamcityBuild,
                                 parentTeamcityBuild,
                                 triggeredBy ) );

        RunMacroWorkloadCommand.runReport( workloadParams,
                                           RESULT_DIR,
                                           RESULT_DIR.resolve( "artifacts" ),
                                           STORE_DIR.toUri(),
                                           neo4jConfigFile(),
                                           ErrorReporter.ErrorPolicy.SKIP,
                                           null,
                                           null,
                                           null );
    }

    // Required fields for running Single query
    private static final String QUERY_NAME = null; // "Read 14" (from "ldbc_sf001")

    @Disabled
    @Test
    void executeQuery() throws Exception
    {
        Path workDir = temporaryFolder.absolutePath();
        try ( Resources resources = new Resources( workDir ) )
        {
            Workload workload = Workload.fromName( WORKLOAD_NAME, resources, DEPLOYMENT );
            Query query = workload.queries()
                                  .stream()
                                  .filter( q -> q.name().equals( QUERY_NAME ) )
                                  .findFirst()
                                  .orElseThrow( () -> new RuntimeException( format( "Workload `%s` does not contain query `%s`",
                                                                                    workload.name(), QUERY_NAME ) ) );
            BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( RESULT_DIR, workload.benchmarkGroup() );
            BenchmarkGroupBenchmarkMetricsPrinter printer = new BenchmarkGroupBenchmarkMetricsPrinter( true );
            Jvm jvm = Jvm.bestEffort( JDK_DIR );
            Path dataset = STORE_DIR;
            Store originalStore = Neo4jStore.createFrom( dataset );
            Edition edition = Edition.ENTERPRISE;
            Neo4jDeployment neo4jDeployment = Neo4jDeployment.from( DEPLOYMENT,
                                                                    edition,
                                                                    MEASUREMENT_PARAMS,
                                                                    jvm,
                                                                    dataset,
                                                                    workDir );
            ForkRunner.runForksFor( neo4jDeployment,
                                    groupDir,
                                    query.copyWith( PLANNER ).copyWith( RUNTIME ),
                                    originalStore,
                                    EDITION,
                                    neo4jConfig(),
                                    PROFILERS,
                                    jvm,
                                    FORK_COUNT,
                                    TimeUnit.MILLISECONDS,
                                    printer,
                                    JVM_ARGS,
                                    workDir );
        }
    }

    private Path neo4jConfigFile()
    {
        Path neo4jConfigFile = temporaryFolder.file( "neo4j.conf" );
        Neo4jConfig neo4jConfig = neo4jConfig();
        Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );
        return neo4jConfigFile;
    }

    private Neo4jConfig neo4jConfig()
    {
        // Unless NEO4J_CONFIG points to a real file, this is equivalent to Neo4jConfig.empty()
        return Neo4jConfigBuilder.fromFile( NEO4J_CONFIG )
                                 // Additional settings you wish to run with
                                 .withSetting( GraphDatabaseSettings.allow_upgrade, FALSE )
                                 .build();
    }
}
