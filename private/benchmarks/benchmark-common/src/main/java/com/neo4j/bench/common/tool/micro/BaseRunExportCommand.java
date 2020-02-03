/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.micro;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.AllowedEnumValues;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.neo4j.bench.common.options.Edition.ENTERPRISE;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_BENCHMARK_CONFIG;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_BRANCH_OWNER;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_ERROR_POLICY;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_JMH_ARGS;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_JSON_PATH;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_JVM_ARGS;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_JVM_PATH;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_NEO4J_BRANCH;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_NEO4J_COMMIT;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_NEO4J_CONFIG;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_NEO4J_EDITION;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_NEO4J_VERSION;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_PARENT_TEAMCITY_BUILD;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_PROFILERS;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_PROFILES_DIR;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_STORES_DIR;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_TEAMCITY_BUILD;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_TOOL_BRANCH;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_TOOL_COMMIT;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_TOOL_OWNER;
import static com.neo4j.bench.common.tool.micro.RunExportParams.CMD_TRIGGERED_BY;

public abstract class BaseRunExportCommand implements Runnable
{

    @Option( type = OptionType.COMMAND,
             name = {CMD_JSON_PATH},
             description = "Path to where the JSON is to be generated",
             title = "JSON Path" )
    @Required
    private File jsonPath;

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
             name = {CMD_NEO4J_EDITION},
             description = "Edition of Neo4j that benchmark is run against",
             title = "Neo4j Edition" )
    private Edition neo4jEdition = ENTERPRISE;

    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_BRANCH},
             description = "Neo4j branch name",
             title = "Neo4j Branch" )
    @Required
    private String neo4jBranch;

    @Option( type = OptionType.COMMAND,
             name = {CMD_BRANCH_OWNER},
             description = "Owner of repository containing Neo4j branch",
             title = "Branch Owner" )
    @Required
    private String neo4jBranchOwner;

    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_CONFIG},
             description = "Neo4j configuration used during benchmark",
             title = "Neo4j Configuration" )
    @Required
    private File neo4jConfigFile;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TOOL_COMMIT},
             description = "Commit of benchmarking tool used to run benchmark",
             title = "Benchmark Tool Commit" )
    @Required
    private String toolCommit;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TOOL_OWNER},
             description = "Owner of repository containg the benchmarking tool used to run benchmark",
             title = "Benchmark Tool Owner" )
    @Required
    private String toolOwner = "neo-technology";

    @Option( type = OptionType.COMMAND,
             name = {CMD_TOOL_BRANCH},
             description = "Branch of benchmarking tool used to run benchmark",
             title = "Benchmark Tool Branch" )
    @Required
    private String toolBranch = neo4jVersion;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TEAMCITY_BUILD},
             description = "Build number of the TeamCity build that ran the benchmarks",
             title = "TeamCity Build Number" )
    @Required
    private Long build;

    @Option( type = OptionType.COMMAND,
             name = {CMD_PARENT_TEAMCITY_BUILD},
             description = "Build number of the TeamCity parent build, e.g., Packaging",
             title = "Parent TeamCity Build Number" )
    @Required
    private Long parentBuild;

    @Option( type = OptionType.COMMAND,
             name = {CMD_JVM_ARGS},
             description = "JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC -Xms4g -Xmx4g')",
             title = "JVM Args" )
    private String jvmArgsString = "";

    @Option( type = OptionType.COMMAND,
             name = {CMD_BENCHMARK_CONFIG},
             description = "Benchmark configuration: enable/disable tests, specify parameters to run with",
             title = "Benchmark Configuration" )
    private File benchConfigFile;

    @Option( type = OptionType.COMMAND,
             name = {CMD_JMH_ARGS},
             description = "Standard JMH CLI args. These will be applied on top of other provided configuration",
             title = "JMH Args" )
    private String jmhArgs = "";

    @Option( type = OptionType.COMMAND,
             name = {CMD_PROFILES_DIR},
             description = "Where to collect profiler recordings for executed benchmarks",
             title = "Profile recordings output directory" )
    @Required // this argument is actually required, but only in the case that at least one profiler is enabled
    private File profilerOutput;

    @Option( type = OptionType.COMMAND,
             name = {CMD_PROFILERS},
             description = "Comma separated list of profilers to run with",
             title = "Profilers" )
    private String profilerNames = "";

    @Option( type = OptionType.COMMAND,
             name = {CMD_STORES_DIR},
             description = "Directory where stores, configurations, etc. will be created",
             title = "Stores directory" )
    private File storesDir = Paths.get( "benchmark_stores" ).toFile();

    @Option( type = OptionType.COMMAND,
             name = {CMD_ERROR_POLICY},
             description = "Prescribes how to deal with errors",
             title = "Error Policy" )
    @AllowedEnumValues( ErrorReporter.ErrorPolicy.class )
    private ErrorReporter.ErrorPolicy errorPolicy = ErrorReporter.ErrorPolicy.SKIP;

    @Option( type = OptionType.COMMAND,
             name = {CMD_JVM_PATH},
             description = "Path to JVM -- will also be used when launching fork processes",
             title = "Path to JVM" )
    private File jvmFile;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TRIGGERED_BY},
             description = "Specifies user that triggered this build",
             title = "Specifies user that triggered this build" )
    @Required
    private String triggeredBy;

    public static List<String> argsFor(
            Path jsonPath,
            String neo4jCommit,
            String neo4jVersion,
            Edition neo4jEdition,
            String neo4jBranch,
            String branchOwner,
            Path neo4jConfig,
            String toolCommit,
            String toolBranchOwner,
            String toolBranch,
            long teamcityBuild,
            long parentTeamcityBuild,
            String jvmArgs,
            Path config,
            String jmhArgs,
            Path profilesDir,
            Path storesDir,
            ErrorReporter.ErrorPolicy errorPolicy,
            Jvm jvm,
            String triggeredBy,
            List<ProfilerType> profilers )
    {
        ArrayList<String> commandArgs = Lists.newArrayList(
                "run-export",
                CMD_JSON_PATH,
                jsonPath.toAbsolutePath().toString(),
                CMD_NEO4J_COMMIT,
                neo4jCommit,
                CMD_NEO4J_VERSION,
                neo4jVersion,
                CMD_NEO4J_EDITION,
                neo4jEdition.name(),
                CMD_NEO4J_BRANCH,
                neo4jBranch,
                CMD_BRANCH_OWNER,
                branchOwner,
                CMD_NEO4J_CONFIG,
                neo4jConfig.toAbsolutePath().toString(),
                CMD_TOOL_COMMIT,
                toolCommit,
                CMD_TOOL_OWNER,
                toolBranchOwner,
                CMD_TOOL_BRANCH,
                toolBranch,
                CMD_TEAMCITY_BUILD,
                Long.toString( teamcityBuild ),
                CMD_PARENT_TEAMCITY_BUILD,
                Long.toString( parentTeamcityBuild ),
                CMD_JVM_ARGS,
                jvmArgs,
                CMD_BENCHMARK_CONFIG,
                config.toAbsolutePath().toString(),
                CMD_JMH_ARGS,
                jmhArgs,
                CMD_PROFILES_DIR,
                profilesDir.toAbsolutePath().toString(),
                CMD_STORES_DIR,
                storesDir.toAbsolutePath().toString(),
                CMD_ERROR_POLICY,
                errorPolicy.name(),
                CMD_TRIGGERED_BY,
                triggeredBy,
                CMD_PROFILERS,
                ProfilerType.serializeProfilers( profilers ) );
        if ( jvm.hasPath() )
        {
            commandArgs.add( CMD_JVM_PATH );
            commandArgs.add( jvm.launchJava() );
        }
        return commandArgs;
    }

    @Override
    public void run()
    {
        RunExportParams runExportParams =
                new RunExportParams( jsonPath,
                                     neo4jCommit,
                                     neo4jVersion,
                                     neo4jEdition,
                                     neo4jBranch,
                                     neo4jBranchOwner,
                                     neo4jConfigFile,
                                     toolCommit,
                                     toolOwner,
                                     toolBranch,
                                     build,
                                     parentBuild,
                                     jvmArgsString,
                                     benchConfigFile,
                                     jmhArgs,
                                     profilerOutput,
                                     profilerNames,
                                     storesDir,
                                     errorPolicy,
                                     jvmFile,
                                     triggeredBy );

        doRun( runExportParams );
    }

    protected abstract void doRun( RunExportParams runExportParams );
}
