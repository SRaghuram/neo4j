/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.BenchmarkGroupDirectory;
import com.neo4j.bench.client.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.macro.cli.RunWorkloadCommand;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.process.ForkRunner;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static com.neo4j.bench.client.process.JvmArgs.jvmArgsFromString;
import static java.lang.String.format;

public class ConvenientLocalExecutionIT
{
    // Required fields for running whole Workload or Single query
    private static final Path STORE_DIR = null; // e.g. /Users/you/stores/3.5/ldbc_sf001_data/ not /Users/you/stores/3.5/ldbc_sf001_data/graph.db/
    private static final Path RESULT_DIR = null; // e.g. /Users/you/results/
    private static final String WORKLOAD_NAME = null; // e.g. "ldbc_sf001"
    private static final Neo4jDeployment DEPLOYMENT = Neo4jDeployment.embedded();

    // Optional fields
    private static final boolean SKIP_FLAME_GRAPHS = false;
    private static final Path JDK_DIR = null;
    private static final Path NEO4J_CONFIG = null;
    private static final int FORK_COUNT = 1;
    private static final int WARMUP_COUNT = 1;
    private static final int MEASUREMENT_COUNT = 1;
    private static final List<ProfilerType> PROFILERS = Lists.newArrayList( ProfilerType.JFR );
    private static final Options.ExecutionMode EXECUTION_MODE = Options.ExecutionMode.EXECUTE;
    private static final String JVM_ARGS = "-Xms4g -Xmx4g";
    private static final boolean RECREATE_SCHEMA = false;
    private static final Edition EDITION = Edition.ENTERPRISE;
    private static final Planner PLANNER = Planner.DEFAULT;
    private static final Runtime RUNTIME = Runtime.DEFAULT;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Ignore
    @Test
    public void executeWorkload() throws Exception
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

            Path profilerRecordingsDir = RESULT_DIR.resolve( "profiler_recordings-" + WORKLOAD_NAME );
            Files.createDirectories( profilerRecordingsDir );
            Path resultsJson = RESULT_DIR.resolve( "results-summary.json" );
            List<String> runWorkloadArgs = RunWorkloadCommand.argsFor(
                    RUNTIME,
                    PLANNER,
                    EXECUTION_MODE,
                    WORKLOAD_NAME,
                    store,
                    neo4jConfigFile(),
                    neo4jVersion,
                    neo4jBranch,
                    neo4jCommit,
                    neo4jBranchOwner,
                    toolBranch,
                    toolCommit,
                    toolBranchOwner,
                    RESULT_DIR,
                    PROFILERS,
                    EDITION,
                    Jvm.defaultJvm(),
                    WARMUP_COUNT,
                    MEASUREMENT_COUNT,
                    FORK_COUNT,
                    Duration.ofSeconds( 0 ),
                    Duration.ofMinutes( 10 ),
                    resultsJson,
                    TimeUnit.MICROSECONDS,
                    ErrorPolicy.FAIL,
                    parentTeamcityBuild,
                    teamcityBuild,
                    JVM_ARGS,
                    RECREATE_SCHEMA,
                    profilerRecordingsDir,
                    SKIP_FLAME_GRAPHS,
                    DEPLOYMENT,
                    triggeredBy );
            Main.main( runWorkloadArgs.stream().toArray( String[]::new ) );
        }
    }

    // Required fields for running Single query
    private static final String QUERY_NAME = null; // "Read 14" (from "ldbc_sf001")

    @Ignore
    @Test
    public void executeQuery() throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.newFolder().toPath() ) )
        {
            Workload workload = Workload.fromName( WORKLOAD_NAME, resources, DEPLOYMENT.mode() );
            Query query = workload.queries()
                                  .stream()
                                  .filter( q -> q.name().equals( QUERY_NAME ) )
                                  .findFirst()
                                  .orElseThrow( () -> new RuntimeException( format( "Workload `%s` does not contain query `%s`",
                                                                                    workload.name(), QUERY_NAME ) ) );
            BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( RESULT_DIR, workload.benchmarkGroup() );
            BenchmarkGroupBenchmarkMetricsPrinter printer = new BenchmarkGroupBenchmarkMetricsPrinter( true );
            Jvm jvm = Jvm.bestEffort( JDK_DIR );
            ForkRunner.runForksFor( DEPLOYMENT.launcherFor( Edition.ENTERPRISE,
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
                                    jvmArgsFromString( JVM_ARGS ),
                                    resources );
        }
    }

    private Path neo4jConfigFile() throws Exception
    {
        Path neo4jConfigFile = temporaryFolder.newFile().toPath();
        Neo4jConfig neo4jConfig = neo4jConfig();
        neo4jConfig.writeToFile( neo4jConfigFile );
        return neo4jConfigFile;
    }

    private Neo4jConfig neo4jConfig()
    {
        // Unless NEO4J_CONFIG points to a real file, this is equivalent to Neo4jConfig.empty()
        return Neo4jConfig.fromFile( NEO4J_CONFIG )
                          // Additional settings you wish to run with
                          .withSetting( GraphDatabaseSettings.allow_upgrade, "false" );
    }
}
