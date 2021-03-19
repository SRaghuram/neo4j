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
import com.neo4j.bench.client.env.InstanceDiscovery;
import com.neo4j.bench.client.reporter.ResultsReporter;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.command.ResultsStoreArgs;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.util.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.Schema;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.execution.process.ForkFailureException;
import com.neo4j.bench.macro.execution.process.ForkRunner;
import com.neo4j.bench.macro.execution.process.MeasurementOptions;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.BenchmarkConfig;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.BenchmarkPlan;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.Environment;
import com.neo4j.bench.model.model.Instance;
import com.neo4j.bench.model.model.Java;
import com.neo4j.bench.model.model.Neo4j;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Plan;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;

import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.common.results.ErrorReportingPolicy.REPORT_THEN_FAIL;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_DB_PATH;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_ERROR_POLICY;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_NEO4J_CONFIG;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_WORK_DIR;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.GraphDatabaseSettings.load_csv_file_url_root;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@Command( name = "run-workload", description = "runs all queries for a single workload" )
public class RunMacroWorkloadCommand extends BaseRunWorkloadCommand
{

    private static final Logger LOG = LoggerFactory.getLogger( RunMacroWorkloadCommand.class );

    @Option( type = OptionType.COMMAND,
             name = {CMD_DB_PATH},
             description = "Store directory matching the selected workload. E.g. 'accesscontrol/' not 'accesscontrol/graph.db/'",
             title = "Store directory" )
    @Required
    File storeDir;

    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_CONFIG},
             description = "Neo4j configuration file",
             title = "Neo4j configuration file" )
    @Required
    private File neo4jConfigFile;

    @Option( type = OptionType.COMMAND,
             name = {CMD_WORK_DIR},
             description = "Work directory: where intermediate results, logs, profiler recordings, etc. will be written",
             title = "Work directory" )
    @Required
    protected File workDir;

    @Option( type = OptionType.COMMAND,
             name = {CMD_ERROR_POLICY},
             description = "Specify if execution should terminate on error, or skip and continue",
             title = "Error handling policy" )
    private ErrorReporter.ErrorPolicy errorPolicy = ErrorReporter.ErrorPolicy.SKIP;

    @Option( type = OptionType.COMMAND,
             name = {"--aws-endpoint-url"},
             description = "AWS endpoint URL, used during testing",
             title = "AWS endpoint URL" )
    private String awsEndpointURL;

    @Option( type = OptionType.COMMAND,
             name = "--aws-region",
             description = "AWS region",
             title = "AWS region" )
    private String awsRegion = "eu-north-1";

    @Inject
    private final ResultsStoreArgs resultsStoreArgs = new ResultsStoreArgs();

    public static final String CMD_RECORDINGS_BASE_URI = "--recordings-base-uri";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RECORDINGS_BASE_URI},
             description = "S3 bucket recordings and profiles were uploaded to",
             title = "Recordings and profiles S3 URI" )
    private URI recordingsBaseUri = URI.create( "s3://benchmarking.neo4j.com/recordings/" );

    public static final String CMD_TEST_RUN_ID = "--test-run-id";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TEST_RUN_ID},
             description = "Optional test run identifier",
             title = "Test run identifier" )
    private String testRunId;

    @Override
    protected void doRun( RunMacroWorkloadParams params )
    {
        LOG.debug( "Running report with params {}", params );

        TestRunReport testRunReport = runReport( params,
                                                 workDir.toPath(),
                                                 storeDir.toPath(),
                                                 neo4jConfigFile.toPath(),
                                                 errorPolicy,
                                                 testRunId );
        ResultsReporter resultsReporter = new ResultsReporter( resultsStoreArgs.resultsStoreUsername(),
                                                               resultsStoreArgs.resultsStorePassword(),
                                                               resultsStoreArgs.resultsStoreUri() );

        resultsReporter.reportAndUpload( testRunReport, recordingsBaseUri, workDir, awsEndpointURL, REPORT_THEN_FAIL );
    }

    public static TestRunReport runReport( RunMacroWorkloadParams params,
                                           Path workDir,
                                           Path storeDir,
                                           Path neo4jConfigFile,
                                           ErrorReporter.ErrorPolicy errorPolicy,
                                           String testRunId )
    {
        for ( ParameterizedProfiler profiler : params.profilers() )
        {
            boolean errorOnMissingFlameGraphDependencies = !params.isSkipFlameGraphs();
            profiler.profilerType().assertEnvironmentVariablesPresent( errorOnMissingFlameGraphDependencies );
        }

        MeasurementOptions measurementOptions = new MeasurementOptions( params.warmupCount(),
                                                                        params.measurementCount(),
                                                                        params.minMeasurementDuration(),
                                                                        params.maxMeasurementDuration() );
        Jvm jvm = Jvm.bestEffortOrFail( params.jvm() );
        Neo4jDeployment neo4jDeployment = Neo4jDeployment.from( params.deployment(),
                                                                params.neo4jEdition(),
                                                                measurementOptions,
                                                                jvm,
                                                                storeDir.toAbsolutePath(),
                                                                workDir );
        params.deployment().assertExists();

        try ( Resources resources = new Resources( workDir ) )
        {
            Workload workload = Workload.fromName( params.workloadName(), resources, neo4jDeployment.deployment() );
            Store originalStore = Neo4jStore.createFrom( storeDir.toAbsolutePath(), workload.getDatabaseName() );
            BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( workDir, workload.benchmarkGroup() );

            assertQueryNames( params, workload );

            Neo4jConfig neo4jConfig = prepareConfig( params.executionMode(), neo4jConfigFile );
            verifySchema( storeDir, params.neo4jEdition(), neo4jConfigFile, params.isRecreateSchema(), workload );

            ErrorReporter errorReporter = new ErrorReporter( errorPolicy );
            BenchmarkGroupBenchmarkMetrics allResults = new BenchmarkGroupBenchmarkMetrics();
            List<BenchmarkPlan> resultPlans = new ArrayList<>();
            BenchmarkGroupBenchmarkMetricsPrinter conciseMetricsPrinter = new BenchmarkGroupBenchmarkMetricsPrinter( false );
            Instant start = Instant.now();

            for ( Query query : workload.queries()
                                        .stream()
                                        .filter( query -> params.queryNames().isEmpty()
                                                          || params.queryNames().contains( query.name() ) )
                                        .map( query -> query.copyWith( params.runtime() )
                                                            .copyWith( params.planner() )
                                                            .copyWith( params.executionMode() ) )
                                        .collect( toList() ) )
            {
                try
                {
                    Results results = ForkRunner.runForksFor( neo4jDeployment,
                                                              groupDir,
                                                              query,
                                                              originalStore,
                                                              params.neo4jEdition(),
                                                              neo4jConfig,
                                                              params.profilers(),
                                                              jvm,
                                                              params.measurementForkCount(),
                                                              params.unit(),
                                                              conciseMetricsPrinter,
                                                              params.jvmArgs(),
                                                              workDir );

                    BenchmarkGroupBenchmarkMetrics queryResults = new BenchmarkGroupBenchmarkMetrics();
                    queryResults.add( query.benchmarkGroup(),
                                      query.benchmark(),
                                      results.metrics(),
                                      results.rowMetrics(),
                                      neo4jConfig );
                    allResults.addAll( queryResults );

                    List<Path> planFiles = ForkRunner.benchmarkDirFor( groupDir, query ).plans();
                    // Just sanity check to avoid unnecessary/redundant plan creation
                    if ( planFiles.size() != 1 )
                    {
                        throw new RuntimeException( "Expected to find exactly one exported plan but found: " + planFiles.size() );
                    }
                    // current policy is to retrieve one/any of the serialized plans,
                    // as they are expected to all be the same and ignore plans that create stack overflows
                    Optional<Plan> plan = planFiles.stream().findFirst().flatMap( RunMacroWorkloadCommand::readPlan );
                    plan.ifPresent( realPlan -> resultPlans.add( new BenchmarkPlan( query.benchmarkGroup(), query.benchmark(), realPlan ) ) );
                }
                catch ( ForkFailureException e )
                {
                    LOG.error( format( "\n" +
                                       "***************************************\n" +
                                       "Benchmark Execution Failed!\n" +
                                       "Benchmark: %s\n" +
                                       "See directory for error log: %s\n" +
                                       "%s\n" +
                                       "***************************************\n",
                                       e.query().benchmark().name(),
                                       e.benchmarkDir().toAbsolutePath(),
                                       ErrorReporter.stackTraceToString( e ) ) );
                    errorReporter.recordOrThrow( e, query.benchmarkGroup(), query.benchmark() );
                }
            }
            Instant finish = Instant.now();
            String currentTestRunId = testRunId;
            if ( currentTestRunId == null )
            {
                currentTestRunId = BenchmarkUtil.generateUniqueId();
            }

            TestRun testRun = new TestRun( currentTestRunId,
                                           Duration.between( start, finish ).toMillis(),
                                           start.toEpochMilli(),
                                           params.parentBuild(),
                                           params.teamcityBuild(),
                                           params.triggeredBy() );

            BenchmarkTool tool = new BenchmarkTool( Repository.MACRO_BENCH, params.neo4jCommit(), params.neo4jBranchOwner(), params.neo4jBranch() );

            BenchmarkConfig benchmarkConfig = new BenchmarkConfig( new HashMap<>() );
            Java java = Java.current( params.jvmArgs().toArgsString() );

            InstanceDiscovery instanceDiscovery = InstanceDiscovery.create();
            Instance instance = instanceDiscovery.currentInstance( System.getenv() );

            TestRunReport testRunReport = new TestRunReport(
                    testRun,
                    benchmarkConfig,
                    newHashSet( new Neo4j( params.neo4jCommit(),
                                           params.neo4jVersion().fullVersion(),
                                           params.neo4jEdition(),
                                           params.neo4jBranch(),
                                           params.neo4jBranchOwner() ) ),
                    neo4jConfig,
                    Environment.from( instance ),
                    allResults,
                    tool,
                    java,
                    resultPlans,
                    errorReporter.errors() );

            BenchmarkGroupBenchmarkMetricsPrinter verboseMetricsPrinter = new BenchmarkGroupBenchmarkMetricsPrinter( true );
            LOG.debug( verboseMetricsPrinter.toPrettyString( allResults, errorReporter.errors() ) );

            return testRunReport;
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
            LOG.debug( "Stack overflow while exporting plan, plan will not be exported" );
            return Optional.empty();
        }
    }

    private static void assertQueryNames( RunMacroWorkloadParams params, Workload workload )
    {
        List<String> allQueryNames = workload.queries().stream().map( Query::name ).collect( toList() );
        List<String> matchedQueries = new ArrayList<>( params.queryNames() );
        matchedQueries.removeAll( allQueryNames );
        if ( !matchedQueries.isEmpty() )
        {
            throw new IllegalArgumentException( format( "%s queries not found in workload %s", matchedQueries, workload.name() ) );
        }
    }

    public static Neo4jConfig prepareConfig( ExecutionMode executionMode, Path neo4jConfigFile )
    {
        BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile );
        Neo4jConfigBuilder neo4jConfigBuilder = Neo4jConfigBuilder.withDefaults()
                                                                  .mergeWith( Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build() )
                                                                  .withSetting( GraphDatabaseSettings.cypher_hints_error, TRUE )
                                                                  .removeSetting( load_csv_file_url_root );
        if ( executionMode.equals( ExecutionMode.PLAN ) )
        {
            neo4jConfigBuilder = neo4jConfigBuilder
                    .withSetting( GraphDatabaseSettings.query_cache_size, "0" )
                    .withSetting( GraphDatabaseInternalSettings.data_collector_max_recent_query_count, "0" );
        }
        Neo4jConfig neo4jConfig = neo4jConfigBuilder.build();

        LOG.debug( "Running with Neo4j configuration:\n" + neo4jConfig.toString() );

        return neo4jConfig;
    }

    public static void verifySchema( Path dataset, Edition edition, Path neo4jConfigFile, boolean recreateSchema, Workload workload )
    {
        LOG.debug( "Verifying store..." );
        try ( Store store = Neo4jStore.createFrom( dataset, workload.getDatabaseName() ) )
        {
            EmbeddedDatabase.verifySchema( store, edition, neo4jConfigFile, workload.expectedSchema() );
            if ( recreateSchema )
            {
                LOG.debug( "Preparing to recreate schema..." );
                EmbeddedDatabase.recreateSchema( store, edition, neo4jConfigFile, workload.expectedSchema() );
            }
            LOG.debug( "Store verified" );
            EmbeddedDatabase.verifyStoreFormat( store );
        }
    }
}
