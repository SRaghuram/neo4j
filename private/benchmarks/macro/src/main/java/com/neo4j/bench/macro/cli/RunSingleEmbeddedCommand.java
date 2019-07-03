/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.cli;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.Parameters;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.client.process.HasPid;
import com.neo4j.bench.client.process.Pid;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.Store;
import com.neo4j.bench.macro.execution.Options.ExecutionMode;
import com.neo4j.bench.macro.execution.QueryRunner;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.workload.Query;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.neo4j.bench.macro.execution.Neo4jDeployment.DeploymentMode;
import static java.util.Collections.singletonMap;

@Command( name = "run-single-embedded", description = "runs one query in a new process for a single workload" )
public class RunSingleEmbeddedCommand implements Runnable
{
    private static final String CMD_WORKLOAD = "--workload";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORKLOAD},
            description = "Path to workload configuration file",
            title = "Workload configuration",
            required = true )
    private String workloadName;

    private static final String CMD_QUERY = "--query";
    @Option( type = OptionType.COMMAND,
            name = {CMD_QUERY},
            description = "Name of query, in the Workload configuration",
            title = "Query name",
            required = true )
    private String queryName;

    private static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DB},
            description = "Store directory matching the selected workload. E.g. 'accesscontrol/' not 'accesscontrol/graph.db/'",
            title = "Store directory",
            required = true )
    private File storeDir;

    private static final String CMD_EDITION = "--db-edition";
    @Option( type = OptionType.COMMAND,
            name = {CMD_EDITION},
            description = "Neo4j edition: COMMUNITY or ENTERPRISE",
            title = "Neo4j edition",
            required = false )
    private Edition edition = Edition.ENTERPRISE;

    private static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_CONFIG},
            title = "Neo4j configuration file",
            required = false )
    private File neo4jConfigFile;

    private static final String CMD_OUTPUT = "--output";
    @Option( type = OptionType.COMMAND,
            name = {CMD_OUTPUT},
            description = "Output directory: where result will be written",
            title = "Output directory",
            required = true )
    private File outputDir;

    private static final String CMD_PROFILERS = "--profilers";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILERS},
            description = "Comma separated list of profilers to run with",
            title = "Profilers",
            required = false )
    private String profilerNames = "";

    private static final String CMD_PLANNER = "--planner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PLANNER},
            title = "Cypher planner",
            required = false )
    private Planner planner = Planner.DEFAULT;

    private static final String CMD_RUNTIME = "--runtime";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RUNTIME},
            title = "Cypher runtime",
            required = false )
    private Runtime runtime = Runtime.DEFAULT;

    private static final String CMD_MODE = "--mode";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MODE},
            description = "Execution mode: EXECUTE (latency), PLAN (latency)",
            title = "Execution mode",
            required = false )
    private ExecutionMode executionMode = ExecutionMode.EXECUTE;

    private static final String CMD_WARMUP_COUNT = "--warmup-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WARMUP_COUNT},
            title = "Warmup execution count",
            required = true )
    private int warmupCount;

    private static final String CMD_MEASUREMENT_COUNT = "--measurement-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MEASUREMENT_COUNT},
            title = "Measurement execution count",
            required = true )
    private int measurementCount;

    private static final String CMD_MIN_MEASUREMENT_SECONDS = "--min-measurement-seconds";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MIN_MEASUREMENT_SECONDS},
            title = "Min measurement execution duration, in seconds",
            required = false )
    private int minMeasurementSeconds = 30; // 30 seconds

    private static final String CMD_MAX_MEASUREMENT_SECONDS = "--max-measurement-seconds";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MAX_MEASUREMENT_SECONDS},
            title = "Max measurement execution duration, in seconds",
            required = false )
    private int maxMeasurementSeconds = 10 * 60; // 10 minutes

    private static final String CMD_JVM_PATH = "--jvm";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_PATH},
            description = "Path to JVM with which this process was launched",
            title = "Path to JVM",
            required = true )
    private File jvmFile;

    private static final String CMD_WORK_DIR = "--work-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORK_DIR},
            description = "Work directory",
            title = "Work directory",
            required = true )
    private File workDir = new File( System.getProperty( "user.dir" ) );

    @Override
    public void run()
    {
        // At this point if it was necessary to copy store (due to mutating query) it should have been done already, trust that store is safe to use
        try ( Store store = Store.createFrom( storeDir.toPath() ) )
        {
            if ( neo4jConfigFile != null )
            {
                BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile.toPath() );
            }
            Neo4jConfig neo4jConfig = Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build();
            QueryRunner queryRunner = QueryRunner.queryRunnerFor( executionMode,
                                                                  forkDirectory -> createDatabase( store, edition, neo4jConfig, forkDirectory ) );
            Pid clientPid = HasPid.getPid();
            QueryRunner.runSingleCommand( queryRunner,
                                          Jvm.bestEffortOrFail( jvmFile ),
                                          ForkDirectory.openAt( outputDir.toPath() ),
                                          workloadName,
                                          queryName,
                                          planner,
                                          runtime,
                                          executionMode,
                                          singletonMap( clientPid, Parameters.NONE ),
                                          singletonMap( clientPid, ProfilerType.deserializeProfilers( profilerNames ) ),
                                          warmupCount,
                                          minMeasurementSeconds,
                                          maxMeasurementSeconds,
                                          measurementCount,
                                          DeploymentMode.EMBEDDED,
                                          workDir.toPath() );
        }
    }

    private static Database createDatabase( Store store,
                                            Edition edition,
                                            Neo4jConfig neo4jConfig,
                                            ForkDirectory forkDirectory )
    {
        Path neo4jConfigFile = forkDirectory.create( "neo4j-executing.conf" );
        Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );
        return EmbeddedDatabase.startWith( store, edition, neo4jConfigFile );
    }

    public static List<String> argsFor(
            Query query,
            Store store,
            Edition edition,
            Path neo4jConfig,
            ForkDirectory forkDirectory,
            List<ProfilerType> internalProfilers,
            int warmupCount,
            int measurementCount,
            Duration minMeasurementDuration,
            Duration maxMeasurementDuration,
            Jvm jvm,
            Path workDir )
    {
        ArrayList<String> args = Lists.newArrayList(
                "run-single-embedded",
                CMD_WORKLOAD,
                query.benchmarkGroup().name(),
                CMD_QUERY,
                query.name(),
                CMD_PLANNER,
                query.queryString().planner().name(),
                CMD_RUNTIME,
                query.queryString().runtime().name(),
                CMD_MODE,
                query.queryString().executionMode().name(),
                CMD_DB,
                store.topLevelDirectory().toAbsolutePath().toString(),
                CMD_EDITION,
                edition.name(),
                CMD_OUTPUT,
                forkDirectory.toAbsolutePath(),
                CMD_WARMUP_COUNT,
                Integer.toString( warmupCount ),
                CMD_MEASUREMENT_COUNT,
                Integer.toString( measurementCount ),
                CMD_MIN_MEASUREMENT_SECONDS,
                Long.toString( minMeasurementDuration.getSeconds() ),
                CMD_MAX_MEASUREMENT_SECONDS,
                Long.toString( maxMeasurementDuration.getSeconds() ),
                CMD_JVM_PATH,
                jvm.launchJava(),
                CMD_WORK_DIR,
                workDir.toAbsolutePath().toString() );
        if ( !internalProfilers.isEmpty() )
        {
            args.add( CMD_PROFILERS );
            args.add( ProfilerType.serializeProfilers( internalProfilers ) );
        }
        if ( null != neo4jConfig )
        {
            args.add( CMD_NEO4J_CONFIG );
            args.add( neo4jConfig.toAbsolutePath().toString() );
        }
        return args;
    }
}
