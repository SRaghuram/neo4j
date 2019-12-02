/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.macro;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.ErrorReporter.ErrorPolicy;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_EDITION;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_EXECUTION_MODE;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_FORKS;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_JVM_ARGS;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_JVM_PATH;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_MAX_MEASUREMENT_DURATION;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_MEASUREMENT;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_MIN_MEASUREMENT_DURATION;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_NEO4J_BRANCH;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_NEO4J_COMMIT;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_NEO4J_DEPLOYMENT;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_NEO4J_OWNER;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_NEO4J_VERSION;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_PARENT_TEAMCITY_BUILD;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_PLANNER;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_PROFILERS;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_RECREATE_SCHEMA;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_RUNTIME;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_SKIP_FLAMEGRAPHS;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_TEAMCITY_BUILD;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_TIME_UNIT;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_TOOL_BRANCH;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_TOOL_COMMIT;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_TOOL_OWNER;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_TRIGGERED_BY;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_WARMUP;
import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_WORKLOAD;
import static java.lang.String.format;

public abstract class BaseRunWorkloadCommand implements Runnable
{
    @Option( type = OptionType.COMMAND,
             name = {CMD_WORKLOAD},
             description = "Name of workload to run",
             title = "Workload" )
    @Required
    private String workloadName;

    @Option( type = OptionType.COMMAND,
             name = {CMD_EDITION},
             description = "Neo4j edition: COMMUNITY or ENTERPRISE",
             title = "Neo4j edition" )
    private Edition neo4jEdition = Edition.ENTERPRISE;

    @Option( type = OptionType.COMMAND,
             name = {CMD_JVM_PATH},
             description = "Path to JVM -- will also be used when launching fork processes",
             title = "Path to JVM" )
    @Required
    private File jvmFile;

    @Option( type = OptionType.COMMAND,
             name = {CMD_PROFILERS},
             description = "Comma separated list of profilers to run with",
             title = "Profilers" )
    @Required
    private String profilerNames;

    @Option( type = OptionType.COMMAND,
             name = {CMD_WARMUP},
             title = "Warmup execution count" )
    @Required
    private int warmupCount;

    @Option( type = OptionType.COMMAND,
             name = {CMD_MEASUREMENT},
             title = "Measurement execution count" )
    @Required
    private int measurementCount;

    @Option( type = OptionType.COMMAND,
             name = {CMD_MIN_MEASUREMENT_DURATION},
             title = "Min measurement execution duration, in seconds" )
    private int minMeasurementSeconds = 30; // 30 seconds

    @Option( type = OptionType.COMMAND,
             name = {CMD_MAX_MEASUREMENT_DURATION},
             title = "Max measurement execution duration, in seconds" )
    private int maxMeasurementSeconds = 10 * 60; // 10 minutes

    @Option( type = OptionType.COMMAND,
             name = {CMD_FORKS},
             title = "Fork count" )
    private int measurementForkCount = 1;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TIME_UNIT},
             description = "Time unit to report results in",
             title = "Time unit" )
    private TimeUnit unit = TimeUnit.MICROSECONDS;

    @Option( type = OptionType.COMMAND,
             name = {CMD_RUNTIME},
             description = "Cypher runtime",
             title = "Cypher runtime" )
    private Runtime runtime = Runtime.DEFAULT;

    @Option( type = OptionType.COMMAND,
             name = {CMD_PLANNER},
             description = "Cypher planner",
             title = "Cypher planner" )
    private Planner planner = Planner.DEFAULT;

    @Option( type = OptionType.COMMAND,
             name = {CMD_EXECUTION_MODE},
             description = "How to execute: run VS plan",
             title = "Run vs Plan" )
    @Required
    private ExecutionMode executionMode;

    @Option( type = OptionType.COMMAND,
             name = {CMD_JVM_ARGS},
             description = "JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC -Xms4g -Xmx4g')",
             title = "JVM Args" )
    @Required
    private String jvmArgs;

    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_DEPLOYMENT},
             description = "Valid values: 'embedded' or 'server:<path_to_neo4j_server>'",
             title = "Deployment mode" )
    @Required
    private String deploymentMode;

    @Option( type = OptionType.COMMAND,
             name = {CMD_RECREATE_SCHEMA},
             description = "Drop indexes and constraints, delete index directories (and transaction logs), then recreate indexes and constraints",
             title = "Recreate Schema" )
    private boolean recreateSchema;

    @Option( type = OptionType.COMMAND,
             name = {CMD_SKIP_FLAMEGRAPHS},
             description = "Skip FlameGraph generation",
             title = "Skip FlameGraph generation" )
    private boolean skipFlameGraphs;

    // -----------------------------------------------------------------------
    // Result Client Report Results Args
    // -----------------------------------------------------------------------

    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_COMMIT},
             description = "Commit of Neo4j that benchmark is run against",
             title = "Neo4j Commit" )
    @Required
    private String neo4jCommit;

    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_VERSION},
             description = "Version of Neo4j that benchmark is run against (e.g., '3.0.2')",
             title = "Neo4j Version" )
    @Required
    private String neo4jVersion;

    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_BRANCH},
             description = "Neo4j branch name",
             title = "Neo4j Branch" )
    @Required
    private String neo4jBranch;

    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_OWNER},
             description = "Owner of repository containing Neo4j branch",
             title = "Branch Owner" )
    @Required
    private String neo4jBranchOwner;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TOOL_COMMIT},
             description = "Commit of benchmarking tool used to run benchmark",
             title = "Benchmark Tool Commit" )
    @Required
    private String toolCommit;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TOOL_OWNER},
             description = "Owner of repository containing the benchmarking tool used to run benchmark",
             title = "Benchmark Tool Owner" )
    @Required
    private String toolOwner = "neo-technology";

    @Option( type = OptionType.COMMAND,
             name = {CMD_TOOL_BRANCH},
             description = "Branch of benchmarking tool used to run benchmark",
             title = "Benchmark Tool Branch" )
    @Required
    private String toolBranch;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TEAMCITY_BUILD},
             description = "Build number of the TeamCity build that ran the benchmarks",
             title = "TeamCity Build Number" )
    @Required
    private Long teamcityBuild;

    @Option( type = OptionType.COMMAND,
             name = {CMD_PARENT_TEAMCITY_BUILD},
             description = "Build number of the TeamCity parent build, e.g., Packaging",
             title = "Parent TeamCity Build Number" )
    @Required
    private Long parentBuild;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TRIGGERED_BY},
             description = "Specifies user that triggered this build",
             title = "Specifies user that triggered this build" )
    @Required
    private String triggeredBy;

    @Override
    public final void run()
    {

        try
        {
            LogManager.getLogManager().readConfiguration( BaseRunWorkloadCommand.class.getResourceAsStream( "/bench/logging.properties" ) );
        }
        catch ( SecurityException | IOException e )
        {
            System.out.println( format( "failed to initialize java.util.logging\n %s", e ) );
        }

        Deployment deployment = Deployment.parse( deploymentMode );
        List<ProfilerType> profilers = ProfilerType.deserializeProfilers( profilerNames );
        Duration minMeasurementDuration = Duration.ofSeconds( minMeasurementSeconds );
        Duration maxMeasurementDuration = Duration.ofSeconds( maxMeasurementSeconds );
        JvmArgs jvmArgs = JvmArgs.parse( this.jvmArgs );
        RunWorkloadParams commandParams = new RunWorkloadParams( workloadName,
                                                                 neo4jEdition,
                                                                 jvmFile.toPath(),
                                                                 profilers,
                                                                 warmupCount,
                                                                 measurementCount,
                                                                 minMeasurementDuration,
                                                                 maxMeasurementDuration,
                                                                 measurementForkCount,
                                                                 unit,
                                                                 runtime,
                                                                 planner,
                                                                 executionMode,
                                                                 jvmArgs,
                                                                 recreateSchema,
                                                                 skipFlameGraphs,
                                                                 deployment,
                                                                 neo4jCommit,
                                                                 neo4jVersion,
                                                                 neo4jBranch,
                                                                 neo4jBranchOwner,
                                                                 toolCommit,
                                                                 toolOwner,
                                                                 toolBranch,
                                                                 teamcityBuild,
                                                                 parentBuild,
                                                                 triggeredBy );
        doRun( commandParams );
    }

    protected abstract void doRun( RunWorkloadParams params );
}
