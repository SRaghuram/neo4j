/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.AllowedEnumValues;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.model.BenchmarkConfig;
import com.neo4j.bench.common.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.common.model.BenchmarkTool;
import com.neo4j.bench.common.model.Environment;
import com.neo4j.bench.common.model.Java;
import com.neo4j.bench.common.model.Neo4j;
import com.neo4j.bench.common.model.Neo4jConfig;
import com.neo4j.bench.common.model.Repository;
import com.neo4j.bench.common.model.TestRun;
import com.neo4j.bench.common.model.TestRunReport;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.Runner;
import com.neo4j.bench.jmh.api.config.JmhOptionsUtil;
import com.neo4j.bench.jmh.api.config.SuiteDescription;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.BoltConnector;

import static com.neo4j.bench.common.options.Edition.ENTERPRISE;
import static com.neo4j.bench.common.util.Args.concatArgs;
import static com.neo4j.bench.common.util.Args.splitArgs;
import static com.neo4j.bench.common.util.BenchmarkUtil.tryMkDir;

@Command( name = "run-export", description = "runs benchmarks and exports results as JSON" )
public class RunExportCommand implements Runnable
{
    private static final int[] DEFAULT_THREAD_COUNTS = new int[]{1, Runtime.getRuntime().availableProcessors()};

    private static final String CMD_JSON_PATH = "--json_path";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JSON_PATH},
            description = "Path to where the JSON is to be generated",
            title = "JSON Path" )
    @Required
    private File jsonPath;

    private static final String CMD_NEO4J_COMMIT = "--neo4j_commit";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_COMMIT},
            description = "Commit of Neo4j that benchmark is run against",
            title = "Neo4j Commit" )
    @Required
    private String neo4jCommit;

    private static final String CMD_NEO4J_VERSION = "--neo4j_version";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_VERSION},
            description = "Version of Neo4j that benchmark is run against (e.g., '3.0.2')",
            title = "Neo4j Version" )
    @Required
    private String neo4jVersion;

    private static final String CMD_NEO4J_EDITION = "--neo4j_edition";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_EDITION},
            description = "Edition of Neo4j that benchmark is run against",
            title = "Neo4j Edition" )
    private Edition neo4jEdition = ENTERPRISE;

    private static final String CMD_NEO4J_BRANCH = "--neo4j_branch";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_BRANCH},
            description = "Neo4j branch name",
            title = "Neo4j Branch" )
    @Required
    private String neo4jBranch;

    private static final String CMD_BRANCH_OWNER = "--branch_owner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_BRANCH_OWNER},
            description = "Owner of repository containing Neo4j branch",
            title = "Branch Owner" )
    @Required
    private String neo4jBranchOwner;

    private static final String CMD_NEO4J_CONFIG = "--neo4j_config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_CONFIG},
            description = "Neo4j configuration used during benchmark",
            title = "Neo4j Configuration" )
    @Required
    private File neo4jConfigFile;

    private static final String CMD_TOOL_COMMIT = "--tool_commit";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_COMMIT},
            description = "Commit of benchmarking tool used to run benchmark",
            title = "Benchmark Tool Commit" )
    @Required
    private String toolCommit;

    private static final String CMD_TOOL_OWNER = "--tool_branch_owner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_OWNER},
            description = "Owner of repository containg the benchmarking tool used to run benchmark",
            title = "Benchmark Tool Owner" )
    @Required
    private String toolOwner = "neo-technology";

    private static final String CMD_TOOL_BRANCH = "--tool_branch";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_BRANCH},
            description = "Branch of benchmarking tool used to run benchmark",
            title = "Benchmark Tool Branch" )
    @Required
    private String toolBranch = neo4jVersion;

    private static final String CMD_TEAMCITY_BUILD = "--teamcity_build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEAMCITY_BUILD},
            description = "Build number of the TeamCity build that ran the benchmarks",
            title = "TeamCity Build Number" )
    @Required
    private Long build;

    private static final String CMD_PARENT_TEAMCITY_BUILD = "--parent_teamcity_build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PARENT_TEAMCITY_BUILD},
            description = "Build number of the TeamCity parent build, e.g., Packaging",
            title = "Parent TeamCity Build Number" )
    @Required
    private Long parentBuild;

    private static final String CMD_JVM_ARGS = "--jvm_args";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_ARGS},
            description = "JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC -Xms4g -Xmx4g')",
            title = "JVM Args" )
    private String jvmArgsString = "";

    private static final String CMD_BENCHMARK_CONFIG = "--config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_BENCHMARK_CONFIG},
            description = "Benchmark configuration: enable/disable tests, specify parameters to run with",
            title = "Benchmark Configuration" )
    private File benchConfigFile;

    private static final String CMD_JMH_ARGS = "--jmh";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JMH_ARGS},
            description = "Standard JMH CLI args. These will be applied on top of other provided configuration",
            title = "JMH Args" )
    private String jmhArgs = "";

    private static final String CMD_PROFILES_DIR = "--profiles-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILES_DIR},
            description = "Where to collect profiler recordings for executed benchmarks",
            title = "Profile recordings output directory" )
    @Required // this argument is actually required, but only in the case that at least one profiler is enabled
    private File profilerOutput;

    private static final String CMD_PROFILERS = "--profilers";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILERS},
            description = "Comma separated list of profilers to run with",
            title = "Profilers" )
    private String profilerNames = "";

    private static final String CMD_STORES_DIR = "--stores-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_STORES_DIR},
            description = "Directory where stores, configurations, etc. will be created",
            title = "Stores directory" )
    private File storesDir = Paths.get( "benchmark_stores" ).toFile();

    private static final String CMD_ERROR_POLICY = "--error-policy";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ERROR_POLICY},
            description = "Prescribes how to deal with errors",
            title = "Error Policy" )
    @AllowedEnumValues( ErrorPolicy.class )
    private ErrorPolicy errorPolicy = ErrorPolicy.SKIP;

    private static final String CMD_JVM_PATH = "--jvm";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_PATH},
            description = "Path to JVM -- will also be used when launching fork processes",
            title = "Path to JVM" )
    private File jvmFile;

    private static final String CMD_TRIGGERED_BY = "--triggered-by";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TRIGGERED_BY},
            description = "Specifies user that triggered this build",
            title = "Specifies user that triggered this build" )
    @Required
    private String triggeredBy;

    @Override
    public void run()
    {
        List<ProfilerType> profilers = ProfilerType.deserializeProfilers( profilerNames );
        for ( ProfilerType profiler : profilers )
        {
            boolean errorOnMissingSecondaryEnvironmentVariables = true;
            profiler.assertEnvironmentVariablesPresent( errorOnMissingSecondaryEnvironmentVariables );
        }

        // trim anything like '-M01' from end of Neo4j version string
        neo4jVersion = Version.toSanitizeVersion( neo4jVersion );

        Neo4jConfig baseNeo4jConfig = Neo4jConfigBuilder.withDefaults()
                                                        .mergeWith( Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build() )
                                                        .withSetting( new BoltConnector( "bolt" ).enabled, "false" )
                                                        .build();

        String[] additionalJvmArgs = splitArgs( this.jvmArgsString, " " );
        String[] jvmArgs = concatArgs( additionalJvmArgs, baseNeo4jConfig.getJvmArgs().toArray( new String[0] ) );

        // only used in interactive mode, to apply more (normally unsupported) benchmark annotations to JMH configuration
        boolean extendedAnnotationSupport = false;
        BenchmarksRunner runner = new BenchmarksRunner( baseNeo4jConfig,
                                                        JmhOptionsUtil.DEFAULT_FORK_COUNT,
                                                        JmhOptionsUtil.DEFAULT_ITERATION_COUNT,
                                                        JmhOptionsUtil.DEFAULT_ITERATION_DURATION,
                                                        extendedAnnotationSupport );
        SuiteDescription suiteDescription = Runner.createSuiteDescriptionFor( BenchmarksRunner.class.getPackage().getName(),
                                                                              null == benchConfigFile ? null : benchConfigFile.toPath() );
        ErrorReporter errorReporter = new ErrorReporter( errorPolicy );

        if ( !storesDir.exists() )
        {
            System.out.println( "Creating stores directory: " + storesDir.getAbsolutePath() );
            tryMkDir( storesDir.toPath() );
        }

        Instant start = Instant.now();
        BenchmarkGroupBenchmarkMetrics resultMetrics = runner.run( suiteDescription,
                                                                   profilers,
                                                                   jvmArgs,
                                                                   DEFAULT_THREAD_COUNTS,
                                                                   storesDir.toPath(),
                                                                   errorReporter,
                                                                   splitArgs( this.jmhArgs, " " ),
                                                                   Jvm.bestEffortOrFail( jvmFile ),
                                                                   profilerOutput.toPath() );
        Instant finish = Instant.now();

        try
        {
            System.out.println( "Deleting: " + storesDir.getAbsolutePath() );
            FileUtils.deleteRecursively( storesDir );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Failed to to delete stores directory", e );
        }

        String testRunId = BenchmarkUtil.generateUniqueId();
        TestRun testRun = new TestRun(
                testRunId,
                Duration.between( start, finish ).toMillis(),
                start.toEpochMilli(),
                build,
                parentBuild,
                triggeredBy );
        BenchmarkConfig benchmarkConfig = suiteDescription.toBenchmarkConfig();
        BenchmarkTool tool = new BenchmarkTool( Repository.MICRO_BENCH, toolCommit, toolOwner, toolBranch );
        Java java = Java.current( String.join( " ", jvmArgs ) );

        TestRunReport testRunReport = new TestRunReport(
                testRun,
                benchmarkConfig,
                Sets.newHashSet( new Neo4j( neo4jCommit, neo4jVersion, neo4jEdition, neo4jBranch, neo4jBranchOwner ) ),
                baseNeo4jConfig,
                Environment.current(),
                resultMetrics,
                tool,
                java,
                Lists.newArrayList(),
                errorReporter.errors() );

        System.out.println( "Exporting results as JSON to: " + jsonPath.getAbsolutePath() );
        JsonUtil.serializeJson( jsonPath.toPath(), testRunReport );
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
}
