package com.neo4j.bench.macro.cli;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.client.ClientUtil;
import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.BenchmarkConfig;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.client.model.BenchmarkPlan;
import com.neo4j.bench.client.model.BenchmarkTool;
import com.neo4j.bench.client.model.BranchAndVersion;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Environment;
import com.neo4j.bench.client.model.Java;
import com.neo4j.bench.client.model.Neo4j;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.Plan;
import com.neo4j.bench.client.model.Repository;
import com.neo4j.bench.client.model.TestRun;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.BenchmarkDirectory;
import com.neo4j.bench.client.results.BenchmarkGroupDirectory;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.client.util.JsonUtil;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.execution.process.ForkFailureException;
import com.neo4j.bench.macro.execution.process.ForkRunner;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static com.neo4j.bench.client.process.JvmArgs.jvmArgsFromString;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.load_csv_file_url_root;

@Command( name = "run-workload", description = "runs all queries for a single workload" )
public class RunWorkloadCommand implements Runnable
{
    private static final String CMD_WORKLOAD = "--workload";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORKLOAD},
            description = "Name of workload to run",
            title = "Workload",
            required = true )
    private String workloadName;

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
    private Edition neo4jEdition = Edition.ENTERPRISE;

    private static final String CMD_JVM_PATH = "--jvm";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_PATH},
            description = "Path to JVM -- will also be used when launching fork processes",
            title = "Path to JVM",
            required = true )
    private File jvmFile;

    private static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_CONFIG},
            description = "Neo4j configuration file",
            title = "Neo4j configuration file",
            required = false )
    private File neo4jConfigFile;

    private static final String CMD_WORK_DIR = "--work-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORK_DIR},
            description = "Work directory: where intermediate results, logs, profiler recordings, etc. will be written",
            title = "Work directory",
            required = false )
    private File workDir = new File( System.getProperty( "user.dir" ) );

    private static final String CMD_PROFILERS = "--profilers";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILERS},
            description = "Comma separated list of profilers to run with",
            title = "Profilers",
            required = false )
    private String profilerNames = "";

    private static final String CMD_WARMUP = "--warmup-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WARMUP},
            title = "Warmup execution count",
            required = true )
    private int warmupCount;

    private static final String CMD_MEASUREMENT = "--measurement-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MEASUREMENT},
            title = "Measurement execution count",
            required = true )
    private int measurementCount;

    private static final String CMD_FORKS = "--forks";
    @Option( type = OptionType.COMMAND,
            name = {CMD_FORKS},
            title = "Fork count",
            required = false )
    private int measurementForkCount = 1;

    private static final String CMD_RESULTS_JSON = "--results";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_JSON},
            description = "Path to where the results file will be written",
            title = "Results path",
            required = false )
    private File resultsPath = new File( workDir, "results.json" );

    private static final String CMD_TIME_UNIT = "--time-unit";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TIME_UNIT},
            description = "Time unit to report results in",
            title = "Time unit",
            required = false )
    private TimeUnit unit = TimeUnit.MICROSECONDS;

    private static final String CMD_RUNTIME = "--runtime";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RUNTIME},
            description = "Cypher runtime",
            title = "Cypher runtime",
            required = false )
    private Runtime runtime = Runtime.DEFAULT;

    private static final String CMD_PLANNER = "--planner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PLANNER},
            description = "Cypher planner",
            title = "Cypher planner",
            required = false )
    private Planner planner = Planner.DEFAULT;

    private static final String CMD_EXECUTION_MODE = "--execution-mode";
    @Option( type = OptionType.COMMAND,
            name = {CMD_EXECUTION_MODE},
            description = "How to execute: run VS plan",
            title = "Run vs Plan",
            required = false )
    private Options.ExecutionMode executionMode = Options.ExecutionMode.EXECUTE;

    private static final String CMD_ERROR_POLICY = "--error-policy";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ERROR_POLICY},
            description = "Specify if execution should terminate on error, or skip and continue",
            title = "Error handling policy",
            required = false )
    private ErrorPolicy errorPolicy = ErrorPolicy.SKIP;

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

    private static final String CMD_NEO4J_OWNER = "--neo4j-branch-owner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_OWNER},
            description = "Owner of repository containing Neo4j branch",
            title = "Branch Owner",
            required = true )
    private String neo4jBranchOwner;

    private static final String CMD_TOOL_COMMIT = "--tool-commit";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_COMMIT},
            description = "Commit of benchmarking tool used to run benchmark",
            title = "Benchmark Tool Commit",
            required = true )
    private String toolCommit;

    private static final String CMD_TOOL_OWNER = "--tool-branch-owner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_OWNER},
            description = "Owner of repository containing the benchmarking tool used to run benchmark",
            title = "Benchmark Tool Owner",
            required = true )
    private String toolOwner = "neo-technology";

    private static final String CMD_TOOL_BRANCH = "--tool-branch";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TOOL_BRANCH},
            description = "Branch of benchmarking tool used to run benchmark",
            title = "Benchmark Tool Branch",
            required = true )
    private String toolBranch;

    private static final String CMD_TEAMCITY_BUILD = "--teamcity-build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEAMCITY_BUILD},
            description = "Build number of the TeamCity build that ran the benchmarks",
            title = "TeamCity Build Number",
            required = true )
    private Long teamcityBuild;

    private static final String CMD_PARENT_TEAMCITY_BUILD = "--parent-teamcity-build";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PARENT_TEAMCITY_BUILD},
            description = "Build number of the TeamCity parent build, e.g., Packaging",
            title = "Parent TeamCity Build Number",
            required = true )
    private Long parentBuild;

    private static final String CMD_JVM_ARGS = "--jvm-args";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_ARGS},
            description = "JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC -Xms4g -Xmx4g')",
            title = "JVM Args",
            required = false )
    private String jvmArgs = "";

    private static final String CMD_RECREATE_SCHEMA = "--recreate-schema";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RECREATE_SCHEMA},
            description = "Drop indexes and constraints, delete index directories (and transaction logs), then recreate indexes and constraints",
            title = "Recreate Schema",
            required = false )
    private boolean recreateSchema;

    private static final String CMD_PROFILER_RECORDINGS_DIR = "--profiler-recordings-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILER_RECORDINGS_DIR},
            description = "Where to collect profiler recordings for executed benchmarks",
            title = "Profile recordings output directory",
            required = false )
    private File profilerRecordingsOutput = workDir.toPath().resolve( "profiler_recordings" ).toFile();

    private static final String CMD_SKIP_FLAMEGRAPHS = "--skip-flamegraphs";
    @Option( type = OptionType.COMMAND,
            name = {CMD_SKIP_FLAMEGRAPHS},
            description = "Skip FlameGraph generation",
            title = "Skip FlameGraph generation",
            required = false )
    private boolean skipFlameGraphs;

    static final String CMD_TRIGGERED_BY = "--triggered-by";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TRIGGERED_BY},
            description = "Specifies user that triggered this build",
            title = "Specifies user that triggered this build",
            required = true )
    private String triggeredBy;

    @Override
    public void run()
    {
        try ( Resources resources = new Resources() )
        {
            List<ProfilerType> profilers = ProfilerType.deserializeProfilers( profilerNames );
            for ( ProfilerType profiler : profilers )
            {
                boolean errorOnMissingFlameGraphDependencies = !skipFlameGraphs;
                profiler.assertEnvironmentVariablesPresent( errorOnMissingFlameGraphDependencies );
            }

            Workload workload = Workload.fromName( workloadName, resources );
            BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( workDir.toPath(), workload.benchmarkGroup() );
            Jvm jvm = Jvm.bestEffort( this.jvmFile );
            List<String> jvmArgsList = jvmArgsFromString( this.jvmArgs );

            printDetails( jvm, jvmArgsList, workload, groupDir, profilers );

            Path neo4jConfigPath = (null == neo4jConfigFile) ? null : neo4jConfigFile.toPath();
            if ( neo4jConfigPath != null )
            {
                BenchmarkUtil.assertFileNotEmpty( neo4jConfigPath );
            }
            Neo4jConfig neo4jConfig = Neo4jConfig.withDefaults()
                                                 .mergeWith( Neo4jConfig.fromFile( neo4jConfigPath ) )
                                                 .withSetting( GraphDatabaseSettings.cypher_hints_error, "true" )
                                                 .removeSetting( load_csv_file_url_root );

            System.out.println( "Running with Neo4j configuration:\n" + neo4jConfig.toString() );

            System.out.println( "Verifying store..." );
            try ( Store store = Store.createFrom( storeDir.toPath() ) )
            {
                Database.verifySchema( store, neo4jEdition, neo4jConfigPath, workload.expectedSchema() );
                if ( recreateSchema )
                {
                    System.out.println( "Preparing to recreate schema..." );
                    Database.recreateSchema( store, neo4jEdition, neo4jConfigPath, workload.expectedSchema() );
                }
                System.out.println( "Store verified\n" );
                Database.verifyStoreFormat( store.graphDbDirectory().toFile() );
            }

            ErrorReporter errorReporter = new ErrorReporter( errorPolicy );
            BenchmarkGroupBenchmarkMetrics results = new BenchmarkGroupBenchmarkMetrics();
            List<BenchmarkPlan> resultPlans = new ArrayList<>();
            BenchmarkGroupBenchmarkMetricsPrinter conciseMetricsPrinter = new BenchmarkGroupBenchmarkMetricsPrinter( false );
            Instant start = Instant.now();
            for ( Query query : workload.queries().stream().map( query -> query.copyWith( runtime )
                                                                               .copyWith( planner )
                                                                               .copyWith( executionMode ) ).collect( toList() ) )
            {
                try
                {
                    BenchmarkDirectory benchmarkDir = ForkRunner.runForksFor( groupDir,
                                                                              query,
                                                                              Store.createFrom( storeDir.toPath() ),
                                                                              neo4jEdition,
                                                                              neo4jConfig,
                                                                              profilers,
                                                                              jvm,
                                                                              measurementForkCount,
                                                                              warmupCount,
                                                                              measurementCount,
                                                                              unit,
                                                                              conciseMetricsPrinter,
                                                                              jvmArgsList );

                    BenchmarkGroupBenchmarkMetrics queryResults = new BenchmarkGroupBenchmarkMetrics();
                    queryResults.add( query.benchmarkGroup(),
                                      query.benchmark(),
                                      Results.loadFrom( benchmarkDir ).metrics(),
                                      neo4jConfig );
                    results.addAll( queryResults );

                    List<Path> planFiles = benchmarkDir.plans();
                    // TODO this check can maybe be removed in the future, e.g., if it is too restrictive, but for now it is a useful sanity check
                    if ( planFiles.size() != 1 )
                    {
                        // sanity check to avoid unnecessary/redundant plan creation
                        throw new RuntimeException( "Expected to find exactly one exported plan but found: " + planFiles.size() );
                    }
                    // current policy is to retrieve one/any of the serialized plans,
                    // as they are expected to all be the same and ignore plans that create stack overflows
                    Optional<Plan> plan = planFiles.stream().findFirst().flatMap( RunWorkloadCommand::readPlan );
                    plan.ifPresent( realPlan -> resultPlans.add( new BenchmarkPlan( query.benchmarkGroup(), query.benchmark(), realPlan ) ) );
                }
                catch ( ForkFailureException e )
                {
                    System.out.println( format( "%s failed with error: %s\n" +
                                                "See work directory: %s\n",
                                                e.query().benchmark().name(), e.getCause().getMessage(), e.benchmarkDir().toAbsolutePath() ) );
                    errorReporter.recordOrThrow( e, query.benchmarkGroup(), query.benchmark() );
                }
            }
            Instant finish = Instant.now();
            String testRunId = ClientUtil.generateUniqueId();

            TestRun testRun = new TestRun(
                    testRunId,
                    Duration.between( start, finish ).toMillis(),
                    start.toEpochMilli(),
                    teamcityBuild,
                    parentBuild,
                    triggeredBy );

            BenchmarkTool tool = new BenchmarkTool( Repository.MACRO_BENCH, toolCommit, toolOwner, toolBranch );

            BenchmarkConfig benchmarkConfig = new BenchmarkConfig( new HashMap<>() );
            Java java = Java.current( jvmArgs );
            neo4jVersion = BranchAndVersion.toSanitizeVersion( Repository.NEO4J, neo4jVersion );
            TestRunReport testRunReport = new TestRunReport(
                    testRun,
                    benchmarkConfig,
                    Sets.newHashSet( new Neo4j( neo4jCommit, neo4jVersion, neo4jEdition, neo4jBranch, neo4jBranchOwner ) ),
                    neo4jConfig,
                    Environment.current(),
                    results,
                    tool,
                    java,
                    resultPlans,
                    errorReporter.errors() );

            BenchmarkGroupBenchmarkMetricsPrinter verboseMetricsPrinter = new BenchmarkGroupBenchmarkMetricsPrinter( true );
            System.out.println( verboseMetricsPrinter.toPrettyString( results, errorReporter.errors() ) );
            System.out.println( "Exporting results as JSON to: " + resultsPath.getAbsolutePath() );
            JsonUtil.serializeJson( resultsPath.toPath(), testRunReport );

            System.out.println( "Copying profiler recordings to: " + profilerRecordingsOutput.getAbsolutePath() );
            groupDir.copyProfilerRecordings( profilerRecordingsOutput.toPath() );
        }
    }

    private static Optional<Plan> readPlan( Path planFile )
    {
        try
        {
            return Optional.of( JsonUtil.deserializeJson( planFile, Plan.class ) );
        }
        catch ( StackOverflowError stackOverflowError )
        {
            System.out.println( "Stack overflow while exporting plan, plan will not be exported" );
            return Optional.empty();
        }
    }

    private void printDetails( Jvm jvm,
                               List<String> jvmArgsList,
                               Workload workload,
                               BenchmarkGroupDirectory groupDir,
                               List<ProfilerType> profilerTypes )
    {
        System.out.println( String.format( "Java             : %s\n" +
                                           "JVM args         : %s\n" +
                                           "Workload         : %s\n" +
                                           "Store            : %s\n" +
                                           "Recreate Schema  : %s\n" +
                                           "Edition          : %s\n" +
                                           "Work Dir         : %s\n" +
                                           "Config           : %s\n" +
                                           "Profilers        : %s\n" +
                                           "Planner          : %s\n" +
                                           "Runtime          : %s\n" +
                                           "Mode             : %s\n" +
                                           "Forks            : %s\n" +
                                           "Queries          : %s\n" +
                                           "Warmup           : %s\n" +
                                           "Measure          : %s\n",
                                           jvm,
                                           jvmArgsList,
                                           workload.name(),
                                           storeDir.getAbsolutePath(),
                                           recreateSchema,
                                           neo4jEdition,
                                           groupDir.toAbsolutePath(),
                                           (null == neo4jConfigFile) ? "n/a" : neo4jConfigFile.getAbsolutePath(),
                                           profilerTypes,
                                           planner,
                                           runtime,
                                           executionMode,
                                           measurementForkCount,
                                           workload.queries().size(),
                                           warmupCount,
                                           measurementCount ) );
    }

    public static List<String> argsFor(
            Runtime runtime,
            Planner planner,
            Options.ExecutionMode executionMode,
            String workloadName,
            Store store,
            Path neo4jConfig,
            String neo4jVersion,
            String neo4jBranch,
            String neo4jCommit,
            String neo4jBranchOwner,
            String toolBranch,
            String toolCommit,
            String toolBranchOwner,
            Path workDir,
            List<ProfilerType> profilers,
            Edition edition,
            Jvm jvm,
            int warmupCount,
            int measurementCount,
            int measurementForkCount,
            Path resultsJson,
            TimeUnit unit,
            ErrorPolicy errorPolicy,
            long parentTeamcityBuild,
            long teamcityBuild,
            String jvmArgs,
            boolean recreateSchema,
            Path profilerRecordingsOutput,
            boolean skipFlameGraphs )
    {
        ArrayList<String> args = Lists.newArrayList(
                "run-workload",
                CMD_WORKLOAD,
                workloadName,
                CMD_DB,
                store.topLevelDirectory().toAbsolutePath().toString(),
                CMD_PROFILERS,
                ProfilerType.serializeProfilers( profilers ),
                CMD_EDITION,
                edition.name(),
                CMD_WARMUP,
                Integer.toString( warmupCount ),
                CMD_MEASUREMENT,
                Integer.toString( measurementCount ),
                CMD_FORKS,
                Integer.toString( measurementForkCount ),
                CMD_TIME_UNIT,
                unit.name(),
                CMD_RUNTIME,
                runtime.name(),
                CMD_PLANNER,
                planner.name(),
                CMD_EXECUTION_MODE,
                executionMode.name(),
                CMD_ERROR_POLICY,
                errorPolicy.name(),
                CMD_NEO4J_VERSION,
                neo4jVersion,
                CMD_NEO4J_BRANCH,
                neo4jBranch,
                CMD_NEO4J_COMMIT,
                neo4jCommit,
                CMD_NEO4J_OWNER,
                neo4jBranchOwner,
                CMD_TOOL_COMMIT,
                toolCommit,
                CMD_TOOL_BRANCH,
                toolBranch,
                CMD_TOOL_OWNER,
                toolBranchOwner,
                CMD_PARENT_TEAMCITY_BUILD,
                Long.toString( parentTeamcityBuild ),
                CMD_TEAMCITY_BUILD,
                Long.toString( teamcityBuild ),
                CMD_JVM_ARGS,
                jvmArgs );
        if ( recreateSchema )
        {
            args.add( CMD_RECREATE_SCHEMA );
        }
        if ( skipFlameGraphs )
        {
            args.add( CMD_SKIP_FLAMEGRAPHS );
        }
        if ( null != workDir )
        {
            args.add( CMD_WORK_DIR );
            args.add( workDir.toAbsolutePath().toString() );
        }
        if ( null != neo4jConfig )
        {
            args.add( CMD_NEO4J_CONFIG );
            args.add( neo4jConfig.toAbsolutePath().toString() );
        }
        if ( jvm.hasPath() )
        {
            args.add( CMD_JVM_PATH );
            args.add( jvm.launchJava() );
        }
        if ( null != resultsJson )
        {
            args.add( CMD_RESULTS_JSON );
            args.add( resultsJson.toAbsolutePath().toString() );
        }
        if ( null != profilerRecordingsOutput )
        {
            args.add( CMD_PROFILER_RECORDINGS_DIR );
            args.add( profilerRecordingsOutput.toAbsolutePath().toString() );
        }
        return args;
    }
}
