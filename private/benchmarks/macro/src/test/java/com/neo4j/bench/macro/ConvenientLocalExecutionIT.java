/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.model.Neo4jConfig;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.common.util.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.common.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.cli.RunWorkloadCommand;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.process.ForkRunner;
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

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
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
    private static final List<ProfilerType> PROFILERS = Lists.newArrayList( ProfilerType.JFR );
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
        try ( Store store = Store.createFrom( STORE_DIR ) )
        {
            String neo4jVersion = "1.2.3";
            String neo4jBranch = "1.2";
            String neo4jBranchOwner = "neo-technology";
            String neo4jCommit = "abcd123";
            String toolBranch = "0.1";
            String toolBranchOwner = "neo-technology";
            String toolCommit = "1234abc";
            long parentTeamcityBuild = 0;
            long teamcityBuild = 1;
            String triggeredBy = "xyz";
            String jobId = "abc123";

            Path profilerRecordingsDir = RESULT_DIR.resolve( "profiler_recordings-" + WORKLOAD_NAME );
            Files.createDirectories( profilerRecordingsDir );
            Path resultsJson = RESULT_DIR.resolve( "results-summary.json" );
            Path jvmPath = Paths.get( Jvm.defaultJvmOrFail().launchJava() );

            List<String> runWorkloadArgs = RunWorkloadCommand.argsFor(
                    store.topLevelDirectory(),
                    neo4jConfigFile(),
                    RESULT_DIR,
                    resultsJson,
                    profilerRecordingsDir,
                    new RunWorkloadParams(
                            WORKLOAD_NAME,
                            EDITION,
                            jvmPath,
                            PROFILERS,
                            WARMUP_COUNT,
                            MEASUREMENT_COUNT,
                            Duration.ofSeconds( 0 ),
                            Duration.ofMinutes( 10 ),
                            FORK_COUNT,
                            TimeUnit.MICROSECONDS,
                            RUNTIME,
                            PLANNER,
                            EXECUTION_MODE,
                            JVM_ARGS,
                            RECREATE_SCHEMA,
                            SKIP_FLAME_GRAPHS,
                            DEPLOYMENT,
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
        }
    }

    // Required fields for running Single query
    private static final String QUERY_NAME = null; // "Read 14" (from "ldbc_sf001")

    @Disabled
    @Test
    void executeQuery() throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath().toPath() ) )
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
            Neo4jDeployment neo4jDeployment = Neo4jDeployment.from( DEPLOYMENT );
            ForkRunner.runForksFor( neo4jDeployment.launcherFor( Edition.ENTERPRISE,
                                                                 WARMUP_COUNT,
                                                                 MEASUREMENT_COUNT,
                                                                 Duration.ofSeconds( 30 ),
                                                                 Duration.ofMinutes( 10 ),
                                                                 jvm ),
                                    groupDir,
                                    query.copyWith( PLANNER ).copyWith( RUNTIME ),
                                    Store.createFrom( STORE_DIR ),
                                    EDITION,
                                    neo4jConfig(),
                                    PROFILERS,
                                    jvm,
                                    FORK_COUNT,
                                    TimeUnit.MILLISECONDS,
                                    printer,
                                    JVM_ARGS,
                                    resources );
        }
    }

    private Path neo4jConfigFile() throws Exception
    {
        Path neo4jConfigFile = temporaryFolder.file( "neo4j.conf" ).toPath();
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
