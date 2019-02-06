/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.BranchAndVersion;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4j;
import com.neo4j.bench.client.model.Repository;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.util.JsonUtil;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.micro.profile.ProfileDescriptor;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.neo4j.bench.client.model.Edition.ENTERPRISE;
import static com.neo4j.bench.client.util.Args.splitArgs;

@Command( name = "run-export", description = "runs benchmarks and exports results as JSON" )
public class RunExportCommand implements Runnable
{
    private static final int[] DEFAULT_THREAD_COUNTS = new int[]{1, Runtime.getRuntime().availableProcessors()};

    private static final String CMD_JSON_PATH = "--json_path";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JSON_PATH},
            description = "Path to where the JSON is to be generated",
            title = "JSON Path",
            required = true )
    private File jsonPath;

    private static final String CMD_NEO4J_COMMIT = "--neo4j_commit";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_COMMIT},
            description = "Commit of Neo4j that benchmark is run against",
            title = "Neo4j Commit",
            required = true )
    private String neo4jCommit;

    private static final String CMD_NEO4J_VERSION = "--neo4j_version";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_VERSION},
            description = "Version of Neo4j that benchmark is run against (e.g., '3.0.2')",
            title = "Neo4j Version",
            required = true )
    private String neo4jVersion;

    private static final String CMD_NEO4J_EDITION = "--neo4j_edition";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_EDITION},
            description = "Edition of Neo4j that benchmark is run against",
            title = "Neo4j Edition",
            required = false )
    private Edition neo4jEdition = ENTERPRISE;

    private static final String CMD_NEO4J_BRANCH = "--neo4j_branch";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_BRANCH},
            description = "Neo4j branch name",
            title = "Neo4j Branch",
            required = true )
    private String neo4jBranch;

    private static final String CMD_BRANCH_OWNER = "--branch_owner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_BRANCH_OWNER},
            description = "Owner of repository containing Neo4j branch",
            title = "Branch Owner",
            required = true )
    private String neo4jBranchOwner;

    private static final String CMD_NEO4J_CONFIG = "--neo4j_config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_CONFIG},
            description = "Neo4j configuration used during benchmark",
            title = "Neo4j Configuration",
            required = false )
    private File neo4jConfigFile;

    private static final String CMD_TOOL_COMMIT = "--tool_commit";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_COMMIT},
            description = "Commit of benchmarking tool used to run benchmark",
            title = "Benchmark Tool Commit",
            required = true )
    private String toolCommit;

    private static final String CMD_TOOL_OWNER = "--tool_branch_owner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_OWNER},
            description = "Owner of repository containg the benchmarking tool used to run benchmark",
            title = "Benchmark Tool Owner",
            required = true )
    private String toolOwner = "neo-technology";

    private static final String CMD_TOOL_BRANCH = "--tool_branch";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_BRANCH},
            description = "Branch of benchmarking tool used to run benchmark",
            title = "Benchmark Tool Branch",
            required = true )
    private String toolBranch = neo4jVersion;

    private static final String CMD_TEAMCITY_BUILD = "--teamcity_build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEAMCITY_BUILD},
            description = "Build number of the TeamCity build that ran the benchmarks",
            title = "TeamCity Build Number",
            required = true )
    private Long build;

    private static final String CMD_PARENT_TEAMCITY_BUILD = "--parent_teamcity_build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PARENT_TEAMCITY_BUILD},
            description = "Build number of the TeamCity parent build, e.g., Packaging",
            title = "Parent TeamCity Build Number",
            required = true )
    private Long parentBuild;

    private static final String CMD_JVM_ARGS = "--jvm_args";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_ARGS},
            description = "JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC -Xms4g -Xmx4g')",
            title = "JVM Args",
            required = false )
    private String jvmArgs = "";

    private static final String CMD_BENCHMARK_CONFIG = "--config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_BENCHMARK_CONFIG},
            description = "Benchmark configuration: enable/disable tests, specify parameters to run with",
            title = "Benchmark Configuration",
            required = false )
    private File benchConfigFile;

    private static final String CMD_NEO4J_PACKAGE = "--neo4j_package_for_jvm_args";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_PACKAGE},
            description = "Extract default product JVM args from Neo4j tar.gz package",
            title = "Neo4j package containing JVM args",
            required = false )
    private File neo4jPackageForJvmArgs;

    private static final String CMD_JMH_ARGS = "--jmh";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JMH_ARGS},
            description = "Standard JMH CLI args. These will be applied on top of other provided configuration",
            title = "JMH Args",
            required = false )
    private String jmhArgs = "";

    private static final String CMD_PROFILES_DIR = "--profiles-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILES_DIR},
            description = "Where to collect profiler recordings for executed benchmarks",
            title = "Profile recordings output directory",
            // this argument is actually required, but only in the case that at least one profiler is enabled
            required = true )
    private File profilerOutput;

    private static final String CMD_PROFILE_JFR = "--profile-jfr";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILE_JFR},
            description = "Run with JFR profiler",
            title = "Run with JFR profiler",
            required = false )
    private boolean doJfrProfile;

    private static final String CMD_PROFILE_ASYNC = "--profile-async";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILE_ASYNC},
            description = "Run with Async profiler",
            title = "Run with Async profiler",
            required = false )
    private boolean doAsyncProfile;

    private static final String CMD_STORES_DIR = "--stores-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_STORES_DIR},
            description = "Directory where stores, configurations, etc. will be created",
            title = "Stores directory",
            required = false )
    private File storesDir = Paths.get( "benchmark_stores" ).toFile();

    private static final String CMD_ERROR_POLICY = "--error-policy";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ERROR_POLICY},
            description = "Prescribes how to deal with errors",
            title = "Error Policy",
            required = false,
            allowedValues = {"SKIP", "FAIL"} )
    private ErrorPolicy errorPolicy = ErrorPolicy.SKIP;

    private static final String CMD_JVM_PATH = "--jvm";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_PATH},
            description = "Path to JVM -- will also be used when launching fork processes",
            title = "Path to JVM",
            required = false )
    private File jvmFile;

    private static final String CMD_TRIGGERED_BY = "--triggered-by";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TRIGGERED_BY},
            description = "Specifies user that triggered this build",
            title = "Specifies user that triggered this build",
            required = true )
    private String triggeredBy;

    @Override
    public void run()
    {
        List<ProfilerType> profilers = new ArrayList<>();
        if ( doJfrProfile )
        {
            profilers.add( ProfilerType.JFR );
        }
        if ( doAsyncProfile )
        {
            profilers.add( ProfilerType.ASYNC );
        }
        for ( ProfilerType profiler : profilers )
        {
            boolean errorOnMissingSecondaryEnvironmentVariables = true;
            profiler.assertEnvironmentVariablesPresent( errorOnMissingSecondaryEnvironmentVariables );
        }

        // trim anything like '-M01' from end of Neo4j version string
        neo4jVersion = BranchAndVersion.toSanitizeVersion( Repository.NEO4J, neo4jVersion );
        TestRunReport testRun = new BenchmarksRunner().run(
                neo4jConfigFile,
                new Neo4j( neo4jCommit, neo4jVersion, neo4jEdition, neo4jBranch, neo4jBranchOwner ),
                toolCommit,
                toolOwner,
                toolBranch,
                build,
                parentBuild,
                splitArgs( jvmArgs, " " ),
                neo4jPackageForJvmArgs,
                benchConfigFile,
                DEFAULT_THREAD_COUNTS,
                ProfileDescriptor.profileTo( profilerOutput.toPath(), profilers ),
                splitArgs( jmhArgs, " " ),
                storesDir.toPath(),
                errorPolicy,
                Jvm.bestEffortOrFail( jvmFile ),
                triggeredBy );

        System.out.println( "Exporting results as JSON to: " + jsonPath.getAbsolutePath() );
        JsonUtil.serializeJson( jsonPath.toPath(), testRun );
    }

    static List<String> argsFor(
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
            Path neo4jPackageForJvmArgs,
            String jmhArgs,
            Path profilesDir,
            Path storesDir,
            ErrorPolicy errorPolicy,
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
                CMD_NEO4J_PACKAGE,
                neo4jPackageForJvmArgs.toAbsolutePath().toString(),
                CMD_JMH_ARGS,
                jmhArgs,
                CMD_PROFILES_DIR,
                profilesDir.toAbsolutePath().toString(),
                CMD_STORES_DIR,
                storesDir.toAbsolutePath().toString(),
                CMD_ERROR_POLICY,
                errorPolicy.name(),
                CMD_TRIGGERED_BY,
                triggeredBy );
        if ( jvm.hasPath() )
        {
            commandArgs.add( CMD_JVM_PATH );
            commandArgs.add( jvm.launchJava() );
        }
        profilers.forEach( profiler ->
        {
            if ( profilers.contains( ProfilerType.JFR ) )
            {
                commandArgs.add( CMD_PROFILE_JFR );
            }
            else if ( profilers.contains( ProfilerType.ASYNC ) )
            {
                commandArgs.add( CMD_PROFILE_ASYNC );
            }
            else
            {
                throw new RuntimeException( "Unsupported profiler type: " + profiler );
            }
        } );
        return commandArgs;
    }
}
