/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.HasPid;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.DeploymentMode;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.QueryRunner;
import com.neo4j.bench.macro.execution.database.ServerDatabase;
import com.neo4j.bench.macro.execution.process.InternalProfilerAssist;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Command( name = "run-single-server", description = "runs one query in a new process for a single workload" )
public class RunSingleServerCommand implements Runnable
{
    private static final String CMD_WORKLOAD = "--workload";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORKLOAD},
            description = "Path to workload configuration file",
            title = "Workload configuration" )
    @Required
    private String workloadName;

    private static final String CMD_QUERY = "--query";
    @Option( type = OptionType.COMMAND,
            name = {CMD_QUERY},
            description = "Name of query, in the Workload configuration",
            title = "Query name" )
    @Required
    private String queryName;

    private static final String CMD_BOLT_URI = "--bolt-uri";
    @Option( type = OptionType.COMMAND,
            name = {CMD_BOLT_URI},
            description = "Server Bolt URI, E.g., 'bolt:// 127.0.0.1:7687'",
            title = "Bolt URI" )
    @Required
    private URI boltUri;

    private static final String CMD_NEO4J_PID = "--neo4j-pid";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_PID},
            description = "PID of Neo4j Server process",
            title = "Neo4j PID" )
    @Required
    private long neo4jPid;

    private static final String CMD_OUTPUT = "--output";
    @Option( type = OptionType.COMMAND,
            name = {CMD_OUTPUT},
            description = "Output directory: where result will be written",
            title = "Output directory" )
    @Required
    private File outputDir;

    private static final String CMD_CLIENT_PROFILERS = "--client-profilers";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CLIENT_PROFILERS},
            description = "Comma separated list of profilers to profile client process with",
            title = "Client profilers" )
    private String clientProfilerNames = "";

    private static final String CMD_SERVER_PROFILERS = "--server-profilers";
    @Option( type = OptionType.COMMAND,
            name = {CMD_SERVER_PROFILERS},
            description = "Comma separated list of profilers to profile server process with",
            title = "Server profilers" )
    private String serverProfilerNames = "";

    private static final String CMD_PLANNER = "--planner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PLANNER},
            title = "Cypher planner" )
    private Planner planner = Planner.DEFAULT;

    private static final String CMD_RUNTIME = "--runtime";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RUNTIME},
            title = "Cypher runtime" )
    private Runtime runtime = Runtime.DEFAULT;

    private static final String CMD_MODE = "--mode";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MODE},
            description = "Execution mode: EXECUTE (latency), PLAN (latency)",
            title = "Execution mode" )
    private ExecutionMode executionMode = ExecutionMode.EXECUTE;

    private static final String CMD_WARMUP_COUNT = "--warmup-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WARMUP_COUNT},
            title = "Warmup execution count" )
    @Required
    private int warmupCount;

    private static final String CMD_MEASUREMENT_COUNT = "--measurement-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MEASUREMENT_COUNT},
            title = "Measurement execution count" )
    @Required
    private int measurementCount;

    private static final String CMD_MIN_MEASUREMENT_SECONDS = "--min-measurement-seconds";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MIN_MEASUREMENT_SECONDS},
            title = "Min measurement execution duration, in seconds" )
    private int minMeasurementSeconds = 30; // 30 seconds

    private static final String CMD_MAX_MEASUREMENT_SECONDS = "--max-measurement-seconds";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MAX_MEASUREMENT_SECONDS},
            title = "Max measurement execution duration, in seconds" )
    private int maxMeasurementSeconds = 10 * 60; // 10 minutes

    private static final String CMD_JVM_PATH = "--jvm";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_PATH},
            description = "Path to JVM with which this process was launched",
            title = "Path to JVM" )
    @Required
    private File jvmFile;

    private static final String CMD_WORK_DIR = "--work-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORK_DIR},
            description = "Work directory",
            title = "Work directory" )
    @Required
    private File workDir = new File( System.getProperty( "user.dir" ) );

    @Override
    public void run()
    {
        try ( Resources resources = new Resources( workDir.toPath() ) )
        {
            DeploymentMode deploymentMode = Deployment.server();
            Workload workload = Workload.fromName( workloadName, resources, deploymentMode );

            QueryRunner queryRunner = QueryRunner.queryRunnerFor( executionMode,
                                                                  forkDirectory -> ServerDatabase.connectClient( boltUri,
                                                                                                                 workload.getDatabaseName(),
                                                                                                                 new Pid( neo4jPid ) ) );

            QueryRunner.runSingleCommand( queryRunner,
                                          Jvm.bestEffortOrFail( jvmFile ),
                                          ForkDirectory.openAt( outputDir.toPath() ),
                                          workload,
                                          queryName,
                                          planner,
                                          runtime,
                                          executionMode,
                                          InternalProfilerAssist.forLocalServer( neo4jPid,
                                                  ProfilerType.deserializeProfilers( clientProfilerNames ),
                                                  ProfilerType.deserializeProfilers( serverProfilerNames ) ),
                                          warmupCount,
                                          minMeasurementSeconds,
                                          maxMeasurementSeconds,
                                          measurementCount );
        }
    }

    public static List<String> argsFor(
            Query query,
            URI boltUri,
            Pid neo4jPid,
            ForkDirectory forkDirectory,
            List<ProfilerType> clientProfilers,
            List<ProfilerType> serverProfilers,
            int warmupCount,
            int measurementCount,
            Duration minMeasurementDuration,
            Duration maxMeasurementDuration,
            Jvm jvm,
            Path workDir )
    {
        ArrayList<String> args = Lists.newArrayList(
                "run-single-server",
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
                CMD_BOLT_URI,
                boltUri.toString(),
                CMD_NEO4J_PID,
                Long.toString( neo4jPid.get() ),
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
        if ( !clientProfilers.isEmpty() )
        {
            args.add( CMD_CLIENT_PROFILERS );
            args.add( ProfilerType.serializeProfilers( clientProfilers ) );
        }
        if ( !serverProfilers.isEmpty() )
        {
            args.add( CMD_SERVER_PROFILERS );
            args.add( ProfilerType.serializeProfilers( serverProfilers ) );
        }
        return args;
    }
}
