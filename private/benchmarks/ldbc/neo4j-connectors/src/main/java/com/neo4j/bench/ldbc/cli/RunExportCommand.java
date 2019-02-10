/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

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
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkConfig;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkTool;
import com.neo4j.bench.client.model.BranchAndVersion;
import com.neo4j.bench.client.model.Environment;
import com.neo4j.bench.client.model.Java;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.client.model.Neo4j;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.Repository;
import com.neo4j.bench.client.model.TestRun;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.profiling.ExternalProfiler;
import com.neo4j.bench.client.profiling.InternalProfiler;
import com.neo4j.bench.client.profiling.Profiler;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.results.BenchmarkDirectory;
import com.neo4j.bench.client.results.BenchmarkGroupDirectory;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Args;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.JsonUtil;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.JvmVersion;
import com.neo4j.bench.ldbc.cli.RunCommand.LdbcRunConfig;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.profiling.ProfilerRunner;
import com.neo4j.bench.ldbc.utils.Neo4jArchive;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;
import com.neo4j.bench.ldbc.utils.StoreFormat;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.io.IOException;
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

import org.neo4j.kernel.impl.store.format.standard.Standard;

import static com.ldbc.driver.control.ConsoleAndFileDriverConfiguration.fromParamsMap;
import static com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot.fromJson;
import static com.ldbc.driver.util.ClassLoaderHelper.loadWorkload;
import static com.ldbc.driver.util.FileUtils.copyDir;
import static com.ldbc.driver.util.MapUtils.loadPropertiesToMap;
import static com.neo4j.bench.client.model.Edition.ENTERPRISE;
import static com.neo4j.bench.client.util.BenchmarkUtil.assertFileNotEmpty;
import static com.neo4j.bench.client.util.BenchmarkUtil.lessWhiteSpace;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.assertDisallowFormatMigration;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.assertStoreFormatIsSet;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.extractScaleFactor;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.toBenchmarkGroupName;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

@Command(
        name = "run-export",
        description = "Runs workload multiple times, in forked JVMs, by delegating to 'run' command" )
public class RunExportCommand implements Runnable
{
    // ===================================================
    // ===================== Export ======================
    // ===================================================

    private static final String CMD_JSON_OUTPUT = "--json-output";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JSON_OUTPUT},
            description = "Path specifying where to export JSON-formatted results",
            title = "JSON output",
            required = true )
    private File jsonOutput;

    // ===================================================
    // ====================== Neo4j ======================
    // ===================================================

    private static final String CMD_NEO4J_COMMIT = "--neo4j-commit";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_COMMIT},
            description = "Commit of Neo4j that benchmark is run against",
            title = "Neo4j Commit",
            required = true )
    private String neo4jCommit;

    private static final String CMD_NEO4J_VERSION = "--neo4j-version";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_VERSION},
            description = "Version of Neo4j that benchmark is run against (e.g., '3.0.2')",
            title = "Neo4j Version",
            required = true )
    private String neo4jVersion;

    private static final String CMD_NEO4J_BRANCH = "--neo4j-branch";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_BRANCH},
            description = "Neo4j branch name",
            title = "Neo4j Branch",
            required = true )
    private String neo4jBranch;

    private static final String CMD_BRANCH_OWNER = "--neo4j-branch-owner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_BRANCH_OWNER},
            description = "Owner of repository containing Neo4j branch",
            title = "Neo4j Branch Owner",
            required = false )
    private String neo4jBranchOwner = "neo4j";

    private static final String CMD_TEAMCITY_BUILD = "--teamcity_build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEAMCITY_BUILD},
            description = "Build number of the TeamCity build that ran the benchmarks",
            title = "TeamCity Build Number",
            required = true )
    private Long build;

    private static final String CMD_TEAMCITY_PARENT_BUILD = "--teamcity_parent_build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEAMCITY_PARENT_BUILD},
            description = "Build number of the TeamCity parent build, e.g., Packaging",
            title = "TeamCity Parent Build Number",
            required = true )
    private Long parentBuild;

    // ===================================================
    // ====================== Tool =======================
    // ===================================================

    private static final String CMD_TOOL_COMMIT = "--tool-commit";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_COMMIT},
            description = "Commit of benchmarking tool used to run benchmark",
            title = "Benchmark Tool Commit",
            required = true )
    private String toolCommit;

    private static final String CMD_TOOL_BRANCH = "--tool-branch";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_BRANCH},
            description = "Tool branch name",
            title = "Tool Branch",
            required = true )
    private String toolBranch;

    private static final String CMD_TOOL_BRANCH_OWNER = "--tool-branch-owner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_BRANCH_OWNER},
            description = "Owner of repository containing Tool branch",
            title = "Tool Branch Owner",
            required = true )
    private String toolBranchOwner = "neo4j";

    // ===================================================
    // ================ Tool Configuration ===============
    // ===================================================

    private static final String CMD_LDBC_CONFIG = "--ldbc-config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_LDBC_CONFIG},
            description = "LDBC driver configuration file",
            title = "LDBC Config",
            required = true )
    private File ldbcConfigFile;

    private static final String CMD_WRITES = "--writes";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WRITES},
            description = "Write query parameters directory (only required for write workload)",
            title = "Write Parameters",
            required = false )
    private File writeParams;

    private static final String CMD_READS = "--reads";
    @Option( type = OptionType.COMMAND,
            name = {CMD_READS},
            description = "Read query parameters directory",
            title = "Read Parameters",
            required = false )
    private File readParams;

    private static final String CMD_RESULTS_DIR = "--results";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_DIR},
            description = "Benchmark results directory (will be created if does not exist)",
            title = "Results directory",
            required = false )
    private File resultsDir;

    private static final String CMD_READ_THREADS = "--read-threads";
    @Option( type = OptionType.COMMAND,
            name = {CMD_READ_THREADS},
            description = "Number of threads for executing read queries (write thread count is function of dataset)",
            title = "Read thread count",
            required = true )
    private int readThreads = 1;

    private static final String CMD_WARMUP_COUNT = "--warmup-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WARMUP_COUNT},
            description = "Number of operations to run during warmup phase (can be set in LDBC config)",
            title = "Warmup operation count",
            required = false )
    private Long warmupCount;

    private static final String CMD_RUN_COUNT = "--run-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RUN_COUNT},
            description = "Number of operations to run during measurement phase (can be set in LDBC config)",
            title = "Run operation count",
            required = false )
    private Long runCount;

    static final String CMD_TRIGGERED_BY = "--triggered-by";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TRIGGERED_BY},
            description = "Specifies user that triggered this build",
            title = "Specifies user that triggered this build",
            required = true )
    private String triggeredBy;

    // ===================================================
    // ================== Neo4j Configuration ============
    // ===================================================

    private static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_CONFIG},
            description = "Default Neo4j Configuration",
            title = "Default Neo4j Configuration",
            required = true )
    private File neo4jConfigFile;

    private static final String CMD_NEO4J_BENCHMARK_CONFIG = "--neo4j-benchmark-config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_BENCHMARK_CONFIG},
            description = "Benchmark Neo4j Configuration",
            title = "Benchmark Neo4j Configuration",
            required = true )
    private File neo4jBenchmarkConfigFile;

    static final String CMD_JVM_PATH = "--jvm";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_PATH},
            description = "Path to JVM -- will also be used when launching fork processes",
            title = "Path to JVM",
            required = true )
    private File jvmFile;

    private static final String CMD_JVM_ARGS = "--jvm-args";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_ARGS},
            description = "JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC -Xms4g -Xmx4g')",
            title = "JVM Args",
            required = false )
    private String jvmArgs = "";

    private static final String CMD_NEO4J_PACKAGE_FOR_JVM_ARGS = "--neo4j-package-for-jvm-args";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_PACKAGE_FOR_JVM_ARGS},
            description = "Extract default product JVM args from Neo4j tar.gz package",
            title = "Neo4j package containing JVM args",
            required = false )
    private File neo4jPackageForJvmArgs;

    private static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DB},
            description = "Neo4j database (graph.db) to copy into working directory," +
                          "E.g. 'db_sf001_p064_regular_utc_40ce/' not 'store/db_sf001_p064_regular_utc_40ce/'",
            title = "Neo4j database",
            required = true )
    private File sourceDbDir;

    private static final String CMD_CYPHER_PLANNER = "--planner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CYPHER_PLANNER},
            description = "Cypher Planner: DEFAULT, RULE, COST",
            title = "Cypher Planner",
            required = false )
    private PlannerType planner = PlannerType.DEFAULT;

    private static final String CMD_CYPHER_RUNTIME = "--runtime";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CYPHER_RUNTIME},
            description = "Cypher Runtime: DEFAULT, INTERPRETED, COMPILED, SLOTTED",
            title = "Cypher Runtime",
            required = false )
    private RuntimeType runtime = RuntimeType.DEFAULT;

    private static final String CMD_NEO4J_API = "--neo4j-api";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_API},
            description = "Neo4j surface API:\n" +
                          "\tEMBEDDED_CORE,\n" +
                          "\tEMBEDDED_CYPHER,\n" +
                          "\tREMOTE_CYPHER",
            title = "Neo4j API",
            required = false )
    private Neo4jApi neo4jApi = Neo4jApi.EMBEDDED_CORE;

    // ===================================================
    // ============ Suite Runner Configuration ===========
    // ===================================================

    private static final String CMD_LDBC_JAR = "--ldbc-jar";
    @Option( type = OptionType.COMMAND,
            name = {CMD_LDBC_JAR},
            description = "Neo4j LDBC executable .jar file",
            title = "Executable .jar",
            required = true )
    private File neo4jLdbcJar;

    private static final String CMD_REPETITION_COUNT = "--repetition-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_REPETITION_COUNT},
            description = "Number of complete benchmark executions to perform, to average results over",
            title = "Repetition count",
            required = true )
    private int repetitionCount = 1;

    private static final String CMD_PREFIX = "--prefix";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PREFIX},
            description = "Command line prefix",
            title = "Command line prefix",
            required = false )
    private String cliPrefix = "";

    private static final String CMD_REUSE_DB = "--reuse-db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_REUSE_DB},
            description = "Reuse same database for every repetition, otherwise copy new database for each repetition",
            title = "Reuse database",
            required = false )
    private ReusePolicy reuseDb = ReusePolicy.REUSE;

    private static final String CMD_WORKING_DIR = "--working-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORKING_DIR},
            description = "Working directory into which database will be copied",
            title = "Working directory",
            required = true )
    private File workingDir;

    private static final String CMD_TRACE = "--trace";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TRACE},
            description = "Run with various monitoring tools: vmstat, iostat, mpstat, etc.",
            title = "Run with various monitoring tools",
            required = false )
    private boolean doTrace;

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

    private static final String CMD_PROFILE_GC = "--profile-gc";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILE_GC},
            description = "Run with GC logging",
            title = "Run with GC logging",
            required = false )
    private boolean doGcProfile;

    private static final String CMD_PROFILES_DIR = "--profiles-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILES_DIR},
            description = "Top level output directory for profiled forks",
            title = "Profiling output directory",
            required = true )
    private File profilesDir;

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

            System.out.println(
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
                    CMD_NEO4J_PACKAGE_FOR_JVM_ARGS + " : " + ((null == neo4jPackageForJvmArgs) ? null : neo4jPackageForJvmArgs.getAbsolutePath()) + "\n" +
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
                    CMD_TRACE + " : " + doTrace + "\n" +
                    CMD_PROFILE_JFR + " : " + doJfrProfile + "\n" +
                    CMD_PROFILE_ASYNC + " : " + doAsyncProfile + "\n" +
                    CMD_PROFILE_GC + " : " + doGcProfile + "\n" +
                    CMD_PROFILES_DIR + " : " + ((null == profilesDir) ? null : profilesDir.getAbsolutePath()) + "\n" +
                    "==============================================================\n"
            );

            // base profilers are run on every fork
            ArrayList<ProfilerType> baseProfilers = Lists.newArrayList();
            if ( doTrace )
            {
                baseProfilers.add( ProfilerType.STRACE );
                baseProfilers.add( ProfilerType.IO_STAT );
                baseProfilers.add( ProfilerType.MP_STAT );
                baseProfilers.add( ProfilerType.VM_STAT );
                baseProfilers.add( ProfilerType.JVM_LOGGING );
            }
            // additional profilers are run on one, profiling fork
            List<ProfilerType> additionalProfilers = new ArrayList<>();
            if ( doJfrProfile )
            {
                additionalProfilers.add( ProfilerType.JFR );
            }
            if ( doAsyncProfile )
            {
                additionalProfilers.add( ProfilerType.ASYNC );
            }
            if ( doGcProfile )
            {
                additionalProfilers.add( ProfilerType.GC );
            }

            assertFileNotEmpty( neo4jConfigFile.toPath() );
            assertFileNotEmpty( neo4jBenchmarkConfigFile.toPath() );
            assertDisallowFormatMigration( neo4jBenchmarkConfigFile );
            assertStoreFormatIsSet( neo4jBenchmarkConfigFile );

            FileUtils.tryCreateDirs( resultsDir, false );
            jvmArgs = buildJvmArgsString( jvmArgs, neo4jPackageForJvmArgs );

            Neo4jConfig neo4jConfig = Neo4jConfig.fromFile( neo4jConfigFile )
                                                 .mergeWith( Neo4jConfig.fromFile( neo4jBenchmarkConfigFile ) );
            System.out.println( "Merged Neo4j config:\n" + neo4jConfig.toString() );

            Path mergedNeo4jConfigPath = new File( resultsDir, "merged-neo4j.conf" ).toPath();
            neo4jConfig.writeAsProperties( mergedNeo4jConfigPath );
            System.out.println( "Merged Neo4j config file written into: " + mergedNeo4jConfigPath.toString() );

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
                    localDbDir( sourceDbDir, workingDir ),
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
                    triggeredBy );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
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
            String triggeredBy )
    {
        System.out.println( "============================================" );
        System.out.println( "==== Aggregating & Reporting Statistics ====" );
        System.out.println( "============================================" );
        try
        {
            DriverConfiguration ldbcDriverConfig = fromParamsMap( loadPropertiesToMap( ldbcConfigFile ) );
            Workload uninitializedWorkload = loadWorkload( ldbcDriverConfig.workloadClassName() );
            WorkloadResultsSnapshot workloadResults = aggregateResults( uninitializedWorkload, resultsDirectories );

            System.out.println( workloadResults.toJson() );
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
            System.out.println( "Export results to: " + jsonOutput.getAbsolutePath() );
            JsonUtil.serializeJson( jsonOutput.toPath(), testRunReport );
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
            List<ProfilerType> baseProfilers,
            List<ProfilerType> additionalProfilers,
            Jvm jvm )
    {
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( resultsDir.toPath(), benchmarkGroup );
        BenchmarkDirectory benchmarkDir = groupDir.findOrCreate( summaryBenchmark );
        List<ResultsDirectory> resultsDirectories = new ArrayList<>();
        try
        {
            if ( reuseDb.equals( ReusePolicy.REUSE ) )
            {
                copyDir( sourceDbDir, ldbcRunConfig.storeDir );
            }

            for ( ProfilerType profiler : additionalProfilers )
            {
                ArrayList<ProfilerType> profilers = new ArrayList<>( baseProfilers );
                profilers.add( profiler );
                ForkDirectory forkDir = benchmarkDir.create( "profiling-fork-" + profiler.name(), profilers );
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
                System.out.println( format( "Deleting database: %s", ldbcRunConfig.storeDir.getAbsolutePath() ) );
                org.apache.commons.io.FileUtils.deleteQuietly( ldbcRunConfig.storeDir );
            }
        }
        return resultsDirectories;
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
            List<ProfilerType> profilerTypes )
    {
        try
        {
            if ( reuseDb.equals( ReusePolicy.COPY_NEW ) )
            {
                if ( ldbcRunConfig.storeDir.exists() )
                {
                    System.out.println( format( "Deleting database: %s", ldbcRunConfig.storeDir.getAbsolutePath() ) );
                    org.apache.commons.io.FileUtils.deleteDirectory( ldbcRunConfig.storeDir );
                }
                copyDir( sourceDbDir, ldbcRunConfig.storeDir );
            }

            List<InternalProfiler> internalProfilers = new ArrayList<>();
            List<ExternalProfiler> externalProfilers = new ArrayList<>();
            List<Profiler> profilers = new ArrayList<>();
            for ( ProfilerType profilerType : profilerTypes )
            {
                Profiler profiler = profilerType.create();
                if ( profilerType.isInternal() )
                {
                    internalProfilers.add( (InternalProfiler) profiler );
                }
                if ( profilerType.isExternal() )
                {
                    externalProfilers.add( (ExternalProfiler) profiler );
                }
                profilers.add( profiler );
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
        List<String> jvmArgs = Args.splitArgs( jvmArgsString );

        JvmVersion jvmVersion = jvm.version();

        for ( ExternalProfiler profiler : profilers )
        {
            List<String> profilerJvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, summaryBenchmark );
            profilerJvmArgs.stream()
                           .filter( arg -> !jvmArgs.contains( arg ) )
                           .forEach( jvmArgs::add );
        }
        File outputLog = forkDirectory.create( "ldbc-output.txt" ).toFile();
        FileUtils.forceRecreateFile( outputLog );
        List<String> jvmInvokeArgs = new ArrayList<>();
        for ( ExternalProfiler profiler : profilers )
        {
            List<String> profilerJvmInvokeArgs = profiler.jvmInvokeArgs( forkDirectory, benchmarkGroup, summaryBenchmark );
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

        System.out.println( "LDBC Command Args: " + String.join( " ", processArgs ) );
        return new ProcessBuilder( processArgs )
                .inheritIO()
                .redirectOutput( outputLog )
                .start();
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
        neo4jVersion = BranchAndVersion.toSanitizeVersion( Repository.NEO4J, neo4jVersion );
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
                0,
                1,
                workloadResults.totalOperationCount(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput(),
                workloadResults.throughput() );
        workloadMetrics.add( benchmarkGroup, summaryBenchmark, summaryMetrics, neo4jConfig );

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
                    operationMetrics.stdDev(),
                    1,
                    operationMetrics.count(),
                    operationMetrics.percentile25(),
                    operationMetrics.percentile50(),
                    operationMetrics.percentile75(),
                    operationMetrics.percentile90(),
                    operationMetrics.percentile95(),
                    operationMetrics.percentile99(),
                    operationMetrics.percentile99_9() );
            workloadMetrics.add( benchmarkGroup, benchmark, metrics, neo4jConfig );
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

    private static StoreFormat extractStoreFormat( Neo4jConfig neo4jConfig )
    {
        String formatString = neo4jConfig.toMap().get( record_format.name() );
        switch ( formatString )
        {
        case Standard.LATEST_NAME:
            return StoreFormat.STANDARD;
        case HighLimit.NAME:
            return StoreFormat.HIGH_LIMIT;
        default:
            throw new RuntimeException( "Unexpected record format found: " + formatString );
        }
    }

    private static List<String> buildCommandArgs(
            Jvm jvm,
            List<String> jvmInvokeArgs,
            List<String> jvmArgs,
            File ldbcJar,
            List<String> ldbcArgs,
            String cliPrefix,
            String processName )
    {
        List<String> commandArgs = new ArrayList<>( Args.splitArgs( cliPrefix ) );
        commandArgs.addAll( jvmInvokeArgs );
        commandArgs.add( jvm.launchJava() );
        commandArgs.add( "-Dname=" + processName );
        commandArgs.addAll( jvmArgs );
        commandArgs.add( "-jar" );
        commandArgs.add( ldbcJar.getAbsolutePath() );
        commandArgs.addAll( ldbcArgs );
        return commandArgs.stream()
                          .map( BenchmarkUtil::lessWhiteSpace )
                          .map( String::trim )
                          .filter( arg -> !arg.isEmpty() )
                          .collect( toList() );
    }

    private static String buildJvmArgsString( String jvmArgs, File neo4jPackageForJvmArgs )
    {
        String defaultJvmArgsString =
                (null == neo4jPackageForJvmArgs)
                ? ""
                : String.join( " ", extractJvmArgs( neo4jPackageForJvmArgs ) );
        return lessWhiteSpace( jvmArgs + " " + defaultJvmArgsString );
    }

    private static List<String> extractJvmArgs( File neo4jPackage )
    {
        return (null == neo4jPackage) ? Lists.newArrayList() : Neo4jArchive.extractJvmArgs( neo4jPackage );
    }

    private static File localDbDir( File sourceDbDir, File workingDir )
    {
        // create a top level store directory, to copy the graph.db folder into
        // E.g., source/graph.db --> workDir/tempStoreDir/graph.db
        Path tempStoreDir = workingDir.toPath().resolve( "tempStoreDir" );
        BenchmarkUtil.assertDoesNotExist( tempStoreDir );
        File destinationDbDir = tempStoreDir.resolve( sourceDbDir.toPath().getFileName() ).toFile();
        if ( sourceDbDir.getParentFile().equals( workingDir ) )
        {
            throw new RuntimeException( format( "Source database:                    %s\n" +
                                                "Must not be in working directory:   %s",
                                                sourceDbDir.getAbsolutePath(), workingDir.getAbsolutePath() ) );
        }
        return destinationDbDir;
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
            System.out.println( result.toJson() );
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
