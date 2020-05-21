/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.client.ResultsDirectory;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.NullLoggingServiceFactory;
import com.ldbc.driver.runtime.metrics.ContinuousMetricSnapshot;
import com.ldbc.driver.runtime.metrics.MetricsManager;
import com.ldbc.driver.runtime.metrics.OperationMetricsSnapshot;
import com.ldbc.driver.runtime.metrics.ResultsLogReader;
import com.ldbc.driver.runtime.metrics.SimpleResultsLogReader;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsService;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.util.FileUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.Profiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Args;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.cli.RunCommand.LdbcRunConfig;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.profiling.ProfilerRunner;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkConfig;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.BranchAndVersion;
import com.neo4j.bench.model.model.Environment;
import com.neo4j.bench.model.model.Java;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Neo4j;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;
import com.neo4j.bench.reporter.ResultsReporter;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ldbc.driver.control.ConsoleAndFileDriverConfiguration.fromParamsMap;
import static com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot.fromJson;
import static com.ldbc.driver.util.ClassLoaderHelper.loadWorkload;
import static com.ldbc.driver.util.FileUtils.copyDir;
import static com.ldbc.driver.util.MapUtils.loadPropertiesToMap;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileNotEmpty;
import static com.neo4j.bench.common.util.BenchmarkUtil.lessWhiteSpace;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.assertDisallowFormatMigration;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.assertStoreFormatIsSet;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.extractScaleFactor;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.extractStoreFormat;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.toBenchmarkGroupName;
import static com.neo4j.bench.model.options.Edition.ENTERPRISE;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@Command(
        name = "run-export",
        description = "Runs workload multiple times, in forked JVMs, by delegating to 'run' command" )
public class RunReportCommand implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger( RunReportCommand.class );

    // ===================================================
    // ===================== Export ======================
    // ===================================================

    private static final String CMD_JSON_OUTPUT = "--json-output";
    @Option( type = OptionType.COMMAND,
             name = {CMD_JSON_OUTPUT},
             description = "Path specifying where to export JSON-formatted results",
             title = "JSON output" )
    @Required
    private File jsonOutput;

    // ===================================================
    // ====================== Neo4j ======================
    // ===================================================

    private static final String CMD_NEO4J_COMMIT = "--neo4j-commit";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_COMMIT},
             description = "Commit of Neo4j that benchmark is run against",
             title = "Neo4j Commit" )
    @Required
    private String neo4jCommit;

    private static final String CMD_NEO4J_VERSION = "--neo4j-version";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_VERSION},
             description = "Version of Neo4j that benchmark is run against (e.g., '3.0.2')",
             title = "Neo4j Version" )
    @Required
    private String neo4jVersion;

    private static final String CMD_NEO4J_BRANCH = "--neo4j-branch";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_BRANCH},
             description = "Neo4j branch name",
             title = "Neo4j Branch" )
    @Required
    private String neo4jBranch;

    private static final String CMD_BRANCH_OWNER = "--neo4j-branch-owner";
    @Option( type = OptionType.COMMAND,
             name = {CMD_BRANCH_OWNER},
             description = "Owner of repository containing Neo4j branch",
             title = "Neo4j Branch Owner" )
    private String neo4jBranchOwner = "neo4j";

    private static final String CMD_TEAMCITY_BUILD = "--teamcity_build";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TEAMCITY_BUILD},
             description = "Build number of the TeamCity build that ran the benchmarks",
             title = "TeamCity Build Number" )
    @Required
    private Long build;

    private static final String CMD_TEAMCITY_PARENT_BUILD = "--teamcity_parent_build";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TEAMCITY_PARENT_BUILD},
             description = "Build number of the TeamCity parent build, e.g., Packaging",
             title = "TeamCity Parent Build Number" )
    @Required
    private Long parentBuild;

    // ===================================================
    // ====================== Tool =======================
    // ===================================================

    private static final String CMD_TOOL_COMMIT = "--tool-commit";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TOOL_COMMIT},
             description = "Commit of benchmarking tool used to run benchmark",
             title = "Benchmark Tool Commit" )
    @Required
    private String toolCommit;

    private static final String CMD_TOOL_BRANCH = "--tool-branch";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TOOL_BRANCH},
             description = "Tool branch name",
             title = "Tool Branch" )
    @Required
    private String toolBranch;

    private static final String CMD_TOOL_BRANCH_OWNER = "--tool-branch-owner";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TOOL_BRANCH_OWNER},
             description = "Owner of repository containing Tool branch",
             title = "Tool Branch Owner" )
    @Required
    private String toolBranchOwner = "neo4j";

    // ===================================================
    // ================ Tool Configuration ===============
    // ===================================================

    private static final String CMD_LDBC_CONFIG = "--ldbc-config";
    @Option( type = OptionType.COMMAND,
             name = {CMD_LDBC_CONFIG},
             description = "LDBC driver configuration file",
             title = "LDBC Config" )
    @Required
    private File ldbcConfigFile;

    private static final String CMD_WRITES = "--writes";
    @Option( type = OptionType.COMMAND,
             name = {CMD_WRITES},
             description = "Write query parameters directory (only required for write workload)",
             title = "Write Parameters" )
    private File writeParams;

    private static final String CMD_READS = "--reads";
    @Option( type = OptionType.COMMAND,
             name = {CMD_READS},
             description = "Read query parameters directory",
             title = "Read Parameters" )
    private File readParams;

    private static final String CMD_RESULTS_DIR = "--results";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_DIR},
             description = "Benchmark results directory (will be created if does not exist)",
             title = "Results directory" )
    private File resultsDir;

    private static final String CMD_READ_THREADS = "--read-threads";
    @Option( type = OptionType.COMMAND,
             name = {CMD_READ_THREADS},
             description = "Number of threads for executing read queries (write thread count is function of dataset)",
             title = "Read thread count" )
    @Required
    private int readThreads = 1;

    private static final String CMD_WARMUP_COUNT = "--warmup-count";
    @Option( type = OptionType.COMMAND,
             name = {CMD_WARMUP_COUNT},
             description = "Number of operations to run during warmup phase (can be set in LDBC config)",
             title = "Warmup operation count" )
    private Long warmupCount;

    private static final String CMD_RUN_COUNT = "--run-count";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RUN_COUNT},
             description = "Number of operations to run during measurement phase (can be set in LDBC config)",
             title = "Run operation count" )
    private Long runCount;

    static final String CMD_TRIGGERED_BY = "--triggered-by";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TRIGGERED_BY},
             description = "Specifies user that triggered this build",
             title = "Specifies user that triggered this build" )
    @Required
    private String triggeredBy;

    // ===================================================
    // ================== Neo4j Configuration ============
    // ===================================================

    private static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_CONFIG},
             description = "Default Neo4j Configuration",
             title = "Default Neo4j Configuration" )
    @Required
    private File neo4jConfigFile;

    private static final String CMD_NEO4J_BENCHMARK_CONFIG = "--neo4j-benchmark-config";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_BENCHMARK_CONFIG},
             description = "Benchmark Neo4j Configuration",
             title = "Benchmark Neo4j Configuration" )
    @Required
    private File neo4jBenchmarkConfigFile;

    static final String CMD_JVM_PATH = "--jvm";
    @Option( type = OptionType.COMMAND,
             name = {CMD_JVM_PATH},
             description = "Path to JVM -- will also be used when launching fork processes",
             title = "Path to JVM" )
    @Required
    private File jvmFile;

    private static final String CMD_JVM_ARGS = "--jvm-args";
    @Option( type = OptionType.COMMAND,
             name = {CMD_JVM_ARGS},
             description = "JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC -Xms4g -Xmx4g')",
             title = "JVM Args" )
    private String jvmArgs = "";

    private static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
             name = {CMD_DB},
             description = "Neo4j database (graph.db) to copy into working directory," +
                           "E.g. 'db_sf001_p064_regular_utc_40ce/' not 'store/db_sf001_p064_regular_utc_40ce/'",
             title = "Neo4j database" )
    @Required
    private File sourceDbDir;

    private static final String CMD_CYPHER_PLANNER = "--planner";
    @Option( type = OptionType.COMMAND,
             name = {CMD_CYPHER_PLANNER},
             description = "Cypher Planner: DEFAULT, RULE, COST",
             title = "Cypher Planner" )
    private Planner planner = Planner.DEFAULT;

    private static final String CMD_CYPHER_RUNTIME = "--runtime";
    @Option( type = OptionType.COMMAND,
             name = {CMD_CYPHER_RUNTIME},
             description = "Cypher Runtime: DEFAULT, INTERPRETED, SLOTTED",
             title = "Cypher Runtime" )
    private Runtime runtime = Runtime.DEFAULT;

    private static final String CMD_NEO4J_API = "--neo4j-api";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_API},
             description = "Neo4j surface API:\n" +
                           "\tEMBEDDED_CORE,\n" +
                           "\tEMBEDDED_CYPHER,\n" +
                           "\tREMOTE_CYPHER",
             title = "Neo4j API" )
    private Neo4jApi neo4jApi = Neo4jApi.EMBEDDED_CORE;

    // ===================================================
    // ============ Suite Runner Configuration ===========
    // ===================================================

    private static final String CMD_LDBC_JAR = "--ldbc-jar";
    @Option( type = OptionType.COMMAND,
             name = {CMD_LDBC_JAR},
             description = "Neo4j LDBC executable .jar file",
             title = "Executable .jar" )
    @Required
    private File neo4jLdbcJar;

    private static final String CMD_REPETITION_COUNT = "--repetition-count";
    @Option( type = OptionType.COMMAND,
             name = {CMD_REPETITION_COUNT},
             description = "Number of complete benchmark executions to perform, to average results over",
             title = "Repetition count" )
    @Required
    private int repetitionCount = 1;

    private static final String CMD_PREFIX = "--prefix";
    @Option( type = OptionType.COMMAND,
             name = {CMD_PREFIX},
             description = "Command line prefix",
             title = "Command line prefix" )
    private String cliPrefix = "";

    private static final String CMD_REUSE_DB = "--reuse-db";
    @Option( type = OptionType.COMMAND,
             name = {CMD_REUSE_DB},
             description = "Reuse same database for every repetition, otherwise copy new database for each repetition",
             title = "Reuse database" )
    private ReusePolicy reuseDb = ReusePolicy.REUSE;

    private static final String CMD_WORKING_DIR = "--working-dir";
    @Option( type = OptionType.COMMAND,
             name = {CMD_WORKING_DIR},
             description = "Working directory into which database will be copied",
             title = "Working directory" )
    @Required
    private File workingDir;

    private static final String CMD_PROFILERS = "--profilers";
    @Option( type = OptionType.COMMAND,
             name = {CMD_PROFILERS},
             description = "Comma separated list of profilers to run with",
             title = "Profilers" )
    private String profilerNames = "";

    private static final String CMD_TRACE = "--trace";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TRACE},
             description = "Run with various monitoring tools: vmstat, iostat, mpstat, etc.",
             title = "Run with various monitoring tools" )
    private boolean doTrace;

    private static final String CMD_PROFILES_DIR = "--profiles-dir";
    @Option( type = OptionType.COMMAND,
             name = {CMD_PROFILES_DIR},
             description = "Top level output directory for profiled forks",
             title = "Profiling output directory" )
    @Required
    private File profilesDir;

    @Option(
            type = OptionType.COMMAND,
            name = {"--aws-endpoint-url"},
            description = "AWS endpoint URL, used during testing",
            title = "AWS endpoint URL" )
    private String awsEndpointURL;

    @Option(
            type = OptionType.COMMAND,
            name = "--aws-region",
            description = "AWS region",
            title = "AWS region" )
    private String awsRegion = "eu-north-1";

    private static final String CMD_RESULTS_STORE_USER = "--results-store-user";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_USER},
             description = "Username for Neo4j database server that stores benchmarking results",
             title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;

    private static final String CMD_RESULTS_STORE_PASSWORD = "--results-store-pass";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_PASSWORD},
             description = "Password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password" )
    @Required
    private String resultsStorePassword;

    private static final String CMD_RESULTS_STORE_URI = "--results-store-uri";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_URI},
             description = "URI to Neo4j database server for storing benchmarking results",
             title = "Results Store" )
    @Required
    private URI resultsStoreUri;

    public static final String CMD_S3_BUCKET = "--s3-bucket";
    @Option( type = OptionType.COMMAND,
             name = {CMD_S3_BUCKET},
             description = "S3 bucket profiles were uploaded to",
             title = "S3 bucket" )
    @Required
    private String s3Bucket;

    private static final String LDBC_FORK_NAME = "ldbc-fork";

    @Override
    public void run()
    {
        try
        {
            DriverConfiguration ldbcConfig = fromParamsMap( loadPropertiesToMap( ldbcConfigFile ) );
            readParams = RunCommand.getFileArgOrFail(
                    readParams,
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    ldbcConfig );
            resultsDir = RunCommand.getFileArgOrFail(
                    resultsDir,
                    ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG,
                    ldbcConfig );

            Jvm jvm = Jvm.bestEffortOrFail( this.jvmFile );

            // base profilers are run on every fork
            List<ParameterizedProfiler> baseProfilers = ParameterizedProfiler.defaultProfilers( ProfilerType.OOM );
            if ( doTrace )
            {
                baseProfilers.addAll( ParameterizedProfiler.defaultProfilers( ProfilerType.JVM_LOGGING ) );
            }
            // additional profilers are run on one, profiling fork
            List<ParameterizedProfiler> additionalProfilers = ParameterizedProfiler.parse( profilerNames );
            for ( ParameterizedProfiler profiler : additionalProfilers )
            {
                boolean errorOnMissingSecondaryEnvironmentVariables = true;
                profiler.profilerType().assertEnvironmentVariablesPresent( errorOnMissingSecondaryEnvironmentVariables );
            }

            LOG.debug(
                    "==============================================================\n" +
                    CMD_JSON_OUTPUT + " : " + jsonOutput.getAbsolutePath() + "\n" +
                    "--------------------------------------------------------------\n" +
                    CMD_NEO4J_COMMIT + " : " + neo4jCommit + "\n" +
                    CMD_NEO4J_VERSION + " : " + neo4jVersion + "\n" +
                    CMD_NEO4J_BRANCH + " : " + neo4jBranch + "\n" +
                    CMD_BRANCH_OWNER + " : " + neo4jBranchOwner + "\n" +
                    "--------------------------------------------------------------\n" +
                    CMD_TEAMCITY_BUILD + " : " + build + "\n" + CMD_TEAMCITY_PARENT_BUILD + " : " + parentBuild + "\n" +
                    "--------------------------------------------------------------\n" +
                    CMD_TOOL_COMMIT + " : " + toolCommit + "\n" +
                    "--------------------------------------------------------------\n" +
                    CMD_LDBC_CONFIG + " : " + ldbcConfigFile.getAbsolutePath() + "\n" +
                    CMD_WRITES + " : " + ((null == writeParams) ? null : writeParams.getAbsolutePath()) + "\n" +
                    CMD_READS + " : " + readParams.getAbsolutePath() + "\n" +
                    CMD_RESULTS_DIR + " : " + resultsDir.getAbsolutePath() + "\n" +
                    CMD_READ_THREADS + " : " + readThreads + "\n" +
                    CMD_WARMUP_COUNT + " : " + warmupCount + "\n" +
                    CMD_RUN_COUNT + " : " + runCount + "\n" +
                    "--------------------------------------------------------------\n" +
                    CMD_NEO4J_CONFIG + " : " + neo4jConfigFile.getAbsolutePath() + "\n" +
                    CMD_NEO4J_BENCHMARK_CONFIG + " : " + neo4jBenchmarkConfigFile.getAbsolutePath() + "\n" +
                    CMD_JVM_PATH + " : " + jvm + "\n" +
                    CMD_JVM_ARGS + " : " + jvmArgs + "\n" +
                    CMD_DB + " : " + ((null == sourceDbDir) ? null : sourceDbDir.getAbsolutePath()) + "\n" +
                    CMD_CYPHER_PLANNER + " : " + planner + "\n" +
                    CMD_CYPHER_RUNTIME + " : " + runtime + "\n" +
                    CMD_NEO4J_API + " : " + neo4jApi + "\n" +
                    "--------------------------------------------------------------\n" +
                    CMD_LDBC_JAR + " : " + ((null == neo4jLdbcJar) ? null : neo4jLdbcJar.getAbsolutePath()) + "\n" +
                    CMD_REPETITION_COUNT + " : " + repetitionCount + "\n" +
                    CMD_PREFIX + " : " + cliPrefix + "\n" +
                    CMD_REUSE_DB + " : " + reuseDb + "\n" +
                    CMD_WORKING_DIR + " : " + ((null == workingDir) ? null : workingDir.getAbsolutePath()) + "\n" +
                    CMD_PROFILERS + " : " + additionalProfilers + "\n" +
                    CMD_TRACE + " : " + doTrace + "\n" +
                    CMD_PROFILES_DIR + " : " + ((null == profilesDir) ? null : profilesDir.getAbsolutePath()) + "\n" +
                    "==============================================================\n"
            );

            assertFileNotEmpty( neo4jConfigFile.toPath() );
            assertFileNotEmpty( neo4jBenchmarkConfigFile.toPath() );
            Neo4jConfig neo4jConfig = Neo4jConfigBuilder.withDefaults()
                                                        .mergeWith( Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build() )
                                                        .mergeWith( Neo4jConfigBuilder.fromFile( neo4jBenchmarkConfigFile ).build() )
                                                        .build();
            assertDisallowFormatMigration( neo4jConfig );
            assertStoreFormatIsSet( neo4jConfig );

            FileUtils.tryCreateDirs( resultsDir, false );

            jvmArgs = buildJvmArgsString( jvmArgs, neo4jConfig );

            LOG.debug( "Merged Neo4j config:\n" + neo4jConfig.toString() );

            Path mergedNeo4jConfigPath = new File( resultsDir, "merged-neo4j.conf" ).toPath();
            Neo4jConfigBuilder.writeToFile( neo4jConfig, mergedNeo4jConfigPath );
            LOG.debug( "Merged Neo4j config file written into: " + mergedNeo4jConfigPath.toString() );

            Map<String,String> benchmarkParams = new HashMap<>();
            benchmarkParams.put( "api", neo4jApi.name() );
            benchmarkParams.put( "planner", planner.name() );
            benchmarkParams.put( "runtime", runtime.name() );
            benchmarkParams.put( "store_format", extractStoreFormat( neo4jConfig ).name() );
            benchmarkParams.put( "scale_factor", Integer.toString( extractScaleFactor( readParams ) ) );

            String benchmarkGroupName = toBenchmarkGroupName( ldbcConfig );
            BenchmarkGroup benchmarkGroup = new BenchmarkGroup( benchmarkGroupName );
            Benchmark summaryBenchmark = Benchmark.benchmarkFor( "Summary metrics of entire workload",
                                                                 "Summary",
                                                                 Benchmark.Mode.THROUGHPUT,
                                                                 benchmarkParams );

            LdbcRunConfig ldbcRunConfig = new LdbcRunConfig(
                    localDbDir( workingDir ),
                    writeParams,
                    readParams,
                    neo4jApi,
                    planner,
                    runtime,
                    ldbcConfigFile,
                    mergedNeo4jConfigPath.toFile(),
                    readThreads,
                    warmupCount,
                    runCount,
                    null );
            long startTime = System.currentTimeMillis();
            List<ResultsDirectory> resultsDirectories = runBenchmarkRepetitions(
                    benchmarkGroup,
                    summaryBenchmark,
                    ldbcRunConfig,
                    baseProfilers,
                    additionalProfilers,
                    jvm );
            long finishTime = System.currentTimeMillis();
            submitResults(
                    resultsDirectories,
                    startTime,
                    finishTime,
                    benchmarkParams,
                    benchmarkGroup,
                    summaryBenchmark,
                    neo4jConfig,
                    triggeredBy,
                    ldbcConfig );
        }
        catch ( Exception e )
        {
            LOG.error("fata error",e);
            System.exit( 1 );
        }
    }

    private void submitResults(
            List<ResultsDirectory> resultsDirectories,
            long startTime,
            long finishTime,
            Map<String,String> benchmarkParams,
            BenchmarkGroup benchmarkGroup,
            Benchmark summaryBenchmark,
            Neo4jConfig neo4jConfig,
            String triggeredBy,
            DriverConfiguration ldbcDriverConfig )
    {
        LOG.debug( "============================================" );
        LOG.debug( "==== Aggregating & Reporting Statistics ====" );
        LOG.debug( "============================================" );
        try
        {
            Workload uninitializedWorkload = loadWorkload( ldbcDriverConfig.workloadClassName() );
            WorkloadResultsSnapshot workloadResults = aggregateResults( uninitializedWorkload, resultsDirectories );

            LOG.debug( workloadResults.toJson() );
            TestRunReport testRunReport = packageResults(
                    workloadResults,
                    ldbcDriverConfig,
                    startTime,
                    finishTime,
                    benchmarkParams,
                    benchmarkGroup,
                    summaryBenchmark,
                    neo4jConfig,
                    triggeredBy );
            LOG.debug( "Export results to: " + jsonOutput.getAbsolutePath() );
            JsonUtil.serializeJson( jsonOutput.toPath(), testRunReport );

            ResultsReporter resultsReporter = new ResultsReporter( profilesDir,
                                                                   testRunReport,
                                                                   s3Bucket,
                                                                   true,
                                                                   resultsStoreUsername,
                                                                   resultsStorePassword,
                                                                   resultsStoreUri,
                                                                   workingDir,
                                                                   awsEndpointURL );
            resultsReporter.report();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error aggregating & reporting results", e );
        }
    }

    private List<ResultsDirectory> runBenchmarkRepetitions(
            BenchmarkGroup benchmarkGroup,
            Benchmark summaryBenchmark,
            LdbcRunConfig ldbcRunConfig,
            List<ParameterizedProfiler> baseProfilers,
            List<ParameterizedProfiler> additionalProfilers,
            Jvm jvm )
    {
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( resultsDir.toPath(), benchmarkGroup );
        BenchmarkDirectory benchmarkDir = groupDir.findOrCreate( summaryBenchmark );
        List<ResultsDirectory> resultsDirectories = new ArrayList<>();
        try
        {
            if ( reuseDb.equals( ReusePolicy.REUSE ) )
            {
                copyStore( ldbcRunConfig );
            }

            for ( ParameterizedProfiler profiler : additionalProfilers )
            {
                List<ParameterizedProfiler> profilers = new ArrayList<>( baseProfilers );
                profilers.add( profiler );
                ForkDirectory forkDir = benchmarkDir.create( "profiling-fork-" + profiler.profilerType().name(), profilers );
                runBenchmarkRepetition(
                        benchmarkGroup,
                        summaryBenchmark,
                        ldbcRunConfig,
                        forkDir,
                        jvm,
                        jvmArgs,
                        neo4jLdbcJar,
                        cliPrefix,
                        profilers );
            }

            for ( int repetition = 0; repetition < repetitionCount; repetition++ )
            {
                ForkDirectory forkDir = benchmarkDir.create( "fork-" + repetition, Collections.emptyList() );
                runBenchmarkRepetition(
                        benchmarkGroup,
                        summaryBenchmark,
                        ldbcRunConfig,
                        forkDir,
                        jvm,
                        jvmArgs,
                        neo4jLdbcJar,
                        cliPrefix,
                        baseProfilers );
                resultsDirectories.add( ResultsDirectory.fromDirectory( Paths.get( forkDir.toAbsolutePath() ).toFile() ) );
            }

            if ( null != profilesDir )
            {
                BenchmarkUtil.tryMkDir( profilesDir.toPath() );
                // base profilers are run on every fork, we would get duplicate recordings if we copied them all into one folder
                // also they are not interesting to upload to results store, it is enough that we have them in TeamCity artifacts
                Set<RecordingType> excludedRecordingTypes = baseProfilers.stream()
                                                                         .map( ParameterizedProfiler::profilerType )
                                                                         .map( ProfilerType::allRecordingTypes )
                                                                         .flatMap( List::stream )
                                                                         .collect( toSet() );
                groupDir.copyProfilerRecordings( profilesDir.toPath(), excludedRecordingTypes );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error executing LDBC benchmark in new process", e );
        }
        finally
        {
            if ( ldbcRunConfig.storeDir.exists() )
            {
                LOG.debug( format( "Deleting database: %s", ldbcRunConfig.storeDir.getAbsolutePath() ) );
                org.apache.commons.io.FileUtils.deleteQuietly( ldbcRunConfig.storeDir );
            }
        }
        return resultsDirectories;
    }

    private void copyStore( LdbcRunConfig ldbcRunConfig )
    {
        //We need to resolve the subfolder of the store.
        copyDir( sourceDbDir, ldbcRunConfig.storeDir );
    }

    private void runBenchmarkRepetition(
            BenchmarkGroup benchmarkGroup,
            Benchmark summaryBenchmark,
            LdbcRunConfig ldbcRunConfig,
            ForkDirectory forkDirectory,
            Jvm jvm,
            String jvmArgs,
            File neo4jLdbcJar,
            String cliPrefix,
            List<ParameterizedProfiler> profilers )
    {
        try
        {
            if ( reuseDb.equals( ReusePolicy.COPY_NEW ) )
            {
                if ( ldbcRunConfig.storeDir.exists() )
                {
                    LOG.debug( format( "Deleting database: %s", ldbcRunConfig.storeDir.getAbsolutePath() ) );
                    org.apache.commons.io.FileUtils.deleteDirectory( ldbcRunConfig.storeDir );
                }
                copyStore( ldbcRunConfig );
            }

            List<InternalProfiler> internalProfilers = new ArrayList<>();
            List<ExternalProfiler> externalProfilers = new ArrayList<>();
            for ( ParameterizedProfiler parameterizedProfiler : profilers )
            {
                ProfilerType profilerType = parameterizedProfiler.profilerType();
                Profiler profiler = profilerType.create();
                if ( profilerType.isInternal() )
                {
                    internalProfilers.add( (InternalProfiler) profiler );
                }
                if ( profilerType.isExternal() )
                {
                    externalProfilers.add( (ExternalProfiler) profiler );
                }
            }

            File waitForFile = forkDirectory.pathFor( "wait-for-file" ).toFile();
            LdbcRunConfig forkLdbcRunConfig = new LdbcRunConfig(
                    ldbcRunConfig.storeDir,
                    ldbcRunConfig.writeParams,
                    ldbcRunConfig.readParams,
                    ldbcRunConfig.neo4jApi,
                    ldbcRunConfig.planner,
                    ldbcRunConfig.runtime,
                    ldbcRunConfig.ldbcConfig,
                    ldbcRunConfig.neo4jConfig,
                    ldbcRunConfig.readThreads,
                    ldbcRunConfig.warmupCount,
                    ldbcRunConfig.runCount,
                    waitForFile );

            Instant start = Instant.now();

            Process ldbcFork = startLdbcFork(
                    forkLdbcRunConfig,
                    forkDirectory,
                    jvm,
                    jvmArgs,
                    neo4jLdbcJar,
                    cliPrefix,
                    externalProfilers,
                    benchmarkGroup,
                    summaryBenchmark,
                    LDBC_FORK_NAME );

            ProfilerRunner.beforeProcess(
                    forkDirectory,
                    benchmarkGroup,
                    summaryBenchmark,
                    externalProfilers );

            try
            {
                ProfilerRunner.profile(
                        jvm,
                        start,
                        ldbcFork,
                        LDBC_FORK_NAME,
                        forkDirectory,
                        benchmarkGroup,
                        summaryBenchmark,
                        internalProfilers );
            }
            finally
            {
                FileUtils.createOrFail( waitForFile );
            }

            int ldbcForkResultCode = ldbcFork.waitFor();
            if ( ldbcForkResultCode != 0 )
            {
                throw new RuntimeException( "Benchmark execution failed with code: " + ldbcForkResultCode );
            }

            ProfilerRunner.afterProcess(
                    forkDirectory,
                    benchmarkGroup,
                    summaryBenchmark,
                    externalProfilers );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error running LDBC fork", e );
        }
    }

    private static Process startLdbcFork(
            LdbcRunConfig ldbcRunConfig,
            ForkDirectory forkDirectory,
            Jvm jvm,
            String jvmArgsString,
            File neo4jLdbcJar,
            String cliPrefix,
            List<ExternalProfiler> profilers,
            BenchmarkGroup benchmarkGroup,
            Benchmark summaryBenchmark,
            String processName ) throws IOException
    {
        String[] ldbcRunArgs = RunCommand.buildArgs( ldbcRunConfig, Paths.get( forkDirectory.toAbsolutePath() ).toFile() );
        JvmArgs jvmArgs = JvmArgs.parse( jvmArgsString );

        JvmVersion jvmVersion = jvm.version();

        try ( Resources resources = new Resources( Paths.get( forkDirectory.toAbsolutePath() ) ) )
        {
            for ( ExternalProfiler profiler : profilers )
            {
                JvmArgs profilerJvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, summaryBenchmark, Parameters.NONE, resources );
                jvmArgs = jvmArgs.merge( profilerJvmArgs );
            }
            File outputLog = forkDirectory.create( "ldbc-output.txt" ).toFile();
            FileUtils.forceRecreateFile( outputLog );
            List<String> jvmInvokeArgs = new ArrayList<>();
            for ( ExternalProfiler profiler : profilers )
            {
                List<String> profilerJvmInvokeArgs = profiler.invokeArgs( forkDirectory, benchmarkGroup, summaryBenchmark, Parameters.NONE );
                jvmInvokeArgs.addAll( profilerJvmInvokeArgs );
            }

            List<String> processArgs = buildCommandArgs(
                    jvm,
                    jvmInvokeArgs,
                    jvmArgs,
                    neo4jLdbcJar,
                    Lists.newArrayList( ldbcRunArgs ),
                    cliPrefix,
                    processName );

            LOG.debug( "LDBC Command Args: " + String.join( " ", processArgs ) );
            return new ProcessBuilder( processArgs )
                    .inheritIO()
                    .redirectOutput( outputLog )
                    .start();
        }
    }

    private TestRunReport packageResults(
            WorkloadResultsSnapshot workloadResults,
            DriverConfiguration ldbcConfig,
            long startTime,
            long finishTime,
            Map<String,String> benchmarkParams,
            BenchmarkGroup benchmarkGroup,
            Benchmark summaryBenchmark,
            Neo4jConfig neo4jConfig,
            String triggeredBy )
    {
        TestRun testRun = new TestRun( finishTime - startTime, startTime, build, parentBuild, triggeredBy );
        BenchmarkConfig benchmarkConfig = new BenchmarkConfig( ldbcConfig.asMap() );
        neo4jVersion = Version.toSanitizeVersion( neo4jVersion );
        if ( !BranchAndVersion.isPersonalBranch( Repository.NEO4J, neo4jBranchOwner ) )
        {
            BranchAndVersion.assertBranchEqualsSeries( neo4jVersion, neo4jBranch );
        }
        if ( !BranchAndVersion.isPersonalBranch( Repository.LDBC_BENCH, toolBranch ) )
        {
            BranchAndVersion.assertBranchEqualsSeries( neo4jVersion, toolBranch );
        }
        Neo4j neo4j = new Neo4j( neo4jCommit, neo4jVersion, ENTERPRISE, neo4jBranch, neo4jBranchOwner );
        BenchmarkTool tool = new BenchmarkTool( Repository.LDBC_BENCH, toolCommit, toolBranchOwner, toolBranch );
        Java java = Java.current( jvmArgs );

        BenchmarkGroupBenchmarkMetrics workloadMetrics = new BenchmarkGroupBenchmarkMetrics();

        Metrics summaryMetrics = new Metrics(
                TimeUnit.SECONDS,
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.totalOperationCount(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput() );
        workloadMetrics.add( benchmarkGroup, summaryBenchmark, summaryMetrics, null /*auxiliary metrics*/, neo4jConfig );

        for ( OperationMetricsSnapshot operation : workloadResults.allMetrics() )
        {
            Benchmark benchmark = Benchmark.benchmarkFor( operation.name(),
                                                          operation.name(),
                                                          Benchmark.Mode.LATENCY,
                                                          benchmarkParams );
            ContinuousMetricSnapshot operationMetrics = operation.runTimeMetric();
            Metrics metrics = new Metrics(
                    operationMetrics.unit(),
                    operationMetrics.min(),
                    operationMetrics.max(),
                    operationMetrics.mean(),
                    operationMetrics.count(),
                    operationMetrics.percentile25(),
                    operationMetrics.percentile50(),
                    operationMetrics.percentile75(),
                    operationMetrics.percentile90(),
                    operationMetrics.percentile95(),
                    operationMetrics.percentile99(),
                    operationMetrics.percentile99_9() );
            workloadMetrics.add( benchmarkGroup, benchmark, metrics, null /*auxiliary metrics*/, neo4jConfig );
        }

        return new TestRunReport(
                testRun,
                benchmarkConfig,
                Sets.newHashSet( neo4j ),
                neo4jConfig,
                Environment.current(),
                workloadMetrics,
                tool,
                java,
                new ArrayList<>() );
    }

    private static List<String> buildCommandArgs(
            Jvm jvm,
            List<String> jvmInvokeArgs,
            JvmArgs jvmArgs,
            File ldbcJar,
            List<String> ldbcArgs,
            String cliPrefix,
            String processName )
    {
        List<String> commandArgs = new ArrayList<>( Args.splitArgs( cliPrefix ) );
        commandArgs.addAll( jvmInvokeArgs );
        commandArgs.add( jvm.launchJava() );
        commandArgs.add( "-Dname=" + processName );
        commandArgs.addAll( jvmArgs.toArgs() );
        commandArgs.add( "-jar" );
        commandArgs.add( ldbcJar.getAbsolutePath() );
        commandArgs.addAll( ldbcArgs );
        return commandArgs.stream()
                          .map( BenchmarkUtil::lessWhiteSpace )
                          .map( String::trim )
                          .filter( arg -> !arg.isEmpty() )
                          .collect( toList() );
    }

    private static String buildJvmArgsString( String jvmArgs, Neo4jConfig neo4jConfig )
    {
        String defaultJvmArgsString = String.join( " ", neo4jConfig.getJvmArgs() );
        return lessWhiteSpace( jvmArgs + " " + defaultJvmArgsString );
    }

    private static File localDbDir( File workingDir )
    {
        // create a top level store directory, to copy the graph.db folder into
        // E.g., source/graph.db --> workDir/tempStoreDir/graph.db
        Path tempStoreDir = workingDir.toPath().resolve( "tempStoreDir" );
        BenchmarkUtil.assertDoesNotExist( tempStoreDir );
        return tempStoreDir.toFile();
    }

    // ===============================================================================================================
    // ====================================== Aggregate Results Logs =================================================
    // ===============================================================================================================

    private static WorkloadResultsSnapshot aggregateResults(
            Workload workload,
            List<ResultsDirectory> resultsDirectories ) throws Exception
    {
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = workload.operationTypeToClassMapping();
        assertDistinctOperations( operationTypeToClassMapping );

        MetricsManager metricsManager = new MetricsManager(
                new SystemTimeSource(),
                TimeUnit.MICROSECONDS,
                ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                operationTypeToClassMapping,
                new NullLoggingServiceFactory()
        );

        // Throughput aggregation must be done manually, to account for long pauses between repetitions
        long startTime = Long.MAX_VALUE;
        long totalDuration = 0;
        for ( ResultsDirectory resultsDirectory : resultsDirectories )
        {
            WorkloadResultsSnapshot result = fromJson( resultsDirectory.getOrCreateResultsSummaryFile( false ) );
            LOG.debug( result.toJson() );
            startTime = Math.min( startTime, result.startTimeAsMilli() );
            totalDuration += result.latestFinishTimeAsMilli() - result.startTimeAsMilli();

            applyResultsLogToMetricsManager( resultsDirectory, metricsManager );
        }

        WorkloadResultsSnapshot aggregatedResults = metricsManager.snapshot();
        return new WorkloadResultsSnapshot( aggregatedResults.allMetrics(),
                                            startTime,
                                            startTime + totalDuration,
                                            aggregatedResults.totalOperationCount(),
                                            aggregatedResults.unit() );
    }

    private static void applyResultsLogToMetricsManager(
            ResultsDirectory resultsDirectory,
            MetricsManager metricsManager ) throws Exception
    {
        File resultsLog = resultsDirectory.getResultsLogFile( false );
        try ( ResultsLogReader reader = new SimpleResultsLogReader( resultsLog ) )
        {
            metricsManager.applyResultsLog( reader );
        }
    }

    private static void assertDistinctOperations( Map<Integer,Class<? extends Operation>> operationTypeToClassMapping )
    {
        List<Class<? extends Operation>> operations = new ArrayList<>( operationTypeToClassMapping.values() );
        List<Class<? extends Operation>> distinctOperations = operationTypeToClassMapping.values().stream()
                                                                                         .distinct()
                                                                                         .collect( toList() );

        if ( operations.size() != distinctOperations.size() )
        {
            operations.removeAll( distinctOperations );
            throw new RuntimeException(
                    format( "Expected all workload operations to be unique. Found duplicates for: %s", operations ) );
        }
    }
}
