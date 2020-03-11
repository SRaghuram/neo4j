/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.util.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.BenchmarkConfig;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.BenchmarkPlan;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.Environment;
import com.neo4j.bench.model.model.Java;
import com.neo4j.bench.model.model.Neo4j;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Plan;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.model.util.JsonUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.execution.process.ForkFailureException;
import com.neo4j.bench.macro.execution.process.ForkRunner;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.tools.migration.MigrateDataIntoOtherDatabase;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;

import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_BATCH_JOB_ID;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_DB_PATH;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_ERROR_POLICY;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_NEO4J_CONFIG;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_PROFILER_RECORDINGS_DIR;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_RESULTS_JSON;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_WORK_DIR;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.GraphDatabaseSettings.load_csv_file_url_root;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@Command( name = "run-workload", description = "runs all queries for a single workload" )
public class RunMacroWorkloadCommand extends BaseRunWorkloadCommand
{
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
             name = {CMD_RESULTS_JSON},
             description = "Name of file where results will be written. Result file will be written into top level of the working directory",
             title = "Results filename" )
    @Required
    private File resultsJson;

    @Option( type = OptionType.COMMAND,
             name = {CMD_PROFILER_RECORDINGS_DIR},
             description = "Directory where profiler recordings will be collected",
             title = "Profile recordings output directory" )
    @Required
    private File profilerRecordingsOutputDir;

    @Option( type = OptionType.COMMAND,
             name = {CMD_ERROR_POLICY},
             description = "Specify if execution should terminate on error, or skip and continue",
             title = "Error handling policy" )
    private ErrorReporter.ErrorPolicy errorPolicy = ErrorReporter.ErrorPolicy.SKIP;

    @Option( type = OptionType.COMMAND,
             name = {CMD_BATCH_JOB_ID},
             description = "Job ID of the batch infra runner",
             title = "Batch Job Id" )
    private String batchJobId;

    @Override
    protected void doRun( RunMacroWorkloadParams params )
    {
        for ( ParameterizedProfiler profiler : params.profilers() )
        {
            boolean errorOnMissingFlameGraphDependencies = !params.isSkipFlameGraphs();
            profiler.profilerType().assertEnvironmentVariablesPresent( errorOnMissingFlameGraphDependencies );
        }

        Neo4jDeployment neo4jDeployment = Neo4jDeployment.from( params.deployment() );
        params.deployment().assertExists();
        Jvm jvm = Jvm.bestEffortOrFail( params.jvm() );

        try ( Resources resources = new Resources( workDir.toPath() ) )
        {
            Workload workload = Workload.fromName( params.workloadName(), resources, neo4jDeployment.deployment() );
            BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( workDir.toPath(), workload.benchmarkGroup() );

            System.out.println( params );

            assertQueryNames( params, workload );

            BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile.toPath() );
            Neo4jConfigBuilder neo4jConfigBuilder = Neo4jConfigBuilder.withDefaults()
                                                                      .mergeWith( Neo4jConfigBuilder.fromFile( neo4jConfigFile.toPath() ).build() )
                                                                      .withSetting( GraphDatabaseSettings.cypher_hints_error, TRUE )
                                                                      .removeSetting( load_csv_file_url_root );
            if ( params.executionMode().equals( ExecutionMode.PLAN ) )
            {
                neo4jConfigBuilder = neo4jConfigBuilder.withSetting( GraphDatabaseSettings.query_cache_size, "0" );
            }
            Neo4jConfig neo4jConfig = neo4jConfigBuilder.build();

            System.out.println( "Running with Neo4j configuration:\n" + neo4jConfig.toString() );

            System.out.println( "Checking store format..." );
            try ( Store store = Neo4jStore.createFrom( storeDir.toPath() ) )
            {
                if ( !store.isFreki() )
                {
                    System.out.println( "Record store detected, migrating to freki." );
                    Path tmpFrekiStoreDir = null;
                    try
                    {
                        tmpFrekiStoreDir = Files.createTempDirectory( "freki" );
                        MigrateDataIntoOtherDatabase.migrate( storeDir.toPath(), tmpFrekiStoreDir );
                        FileUtils.deleteRecursively( storeDir );
                        FileUtils.moveFile( tmpFrekiStoreDir.toFile(), storeDir );
                    }
                    finally
                    {
                        if ( tmpFrekiStoreDir != null )
                        {
                            FileUtils.deleteRecursively( tmpFrekiStoreDir.toFile() );
                        }
                    }
                }
                else
                {
                    System.out.println( "Already freki format, no migration needed." );
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }

            System.out.println( "Verifying store..." );
            try ( Store store = Neo4jStore.createFrom( storeDir.toPath(), workload.getDatabaseName() ) )
            {
                EmbeddedDatabase.verifySchema( store, params.neo4jEdition(), neo4jConfig, workload.expectedSchema() );
                if ( params.isRecreateSchema() )
                {
                    System.out.println( "Preparing to recreate schema..." );
                    EmbeddedDatabase.recreateSchema( store, params.neo4jEdition(), neo4jConfig, workload.expectedSchema() );
                }
                System.out.println( "Store verified\n" );
//                EmbeddedDatabase.verifyStoreFormat( store );
            }

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
                    BenchmarkDirectory benchmarkDir = ForkRunner.runForksFor( neo4jDeployment.launcherFor( params.neo4jEdition(),
                                                                                                           params.warmupCount(),
                                                                                                           params.measurementCount(),
                                                                                                           params.minMeasurementDuration(),
                                                                                                           params.maxMeasurementDuration(),
                                                                                                           jvm ),
                                                                              groupDir,
                                                                              query,
                                                                              Neo4jStore.createFrom( storeDir.toPath().toAbsolutePath() ),
                                                                              params.neo4jEdition(),
                                                                              neo4jConfig,
                                                                              params.profilers(),
                                                                              jvm,
                                                                              params.measurementForkCount(),
                                                                              params.unit(),
                                                                              conciseMetricsPrinter,
                                                                              params.jvmArgs(),
                                                                              resources );

                    BenchmarkGroupBenchmarkMetrics queryResults = new BenchmarkGroupBenchmarkMetrics();
                    Results results = Results.loadFrom( benchmarkDir );
                    queryResults.add( query.benchmarkGroup(),
                                      query.benchmark(),
                                      results.metrics(),
                                      results.rowMetrics(),
                                      neo4jConfig );
                    allResults.addAll( queryResults );

                    List<Path> planFiles = benchmarkDir.plans();
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
                    System.err.println( format( "\n" +
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
            String testRunId = BenchmarkUtil.generateUniqueId();

            TestRun testRun = new TestRun(
                    testRunId,
                    Duration.between( start, finish ).toMillis(),
                    start.toEpochMilli(),
                    params.parentBuild(),
                    params.parentBuild(),
                    params.triggeredBy() );
            if ( batchJobId != null )
            {
                testRun.setBatchJobId( batchJobId );
            }

            BenchmarkTool tool = new BenchmarkTool( Repository.MACRO_BENCH, params.toolCommit(), params.toolOwner(), params.toolBranch() );

            BenchmarkConfig benchmarkConfig = new BenchmarkConfig( new HashMap<>() );
            Java java = Java.current( params.jvmArgs().toArgsString() );
            TestRunReport testRunReport = new TestRunReport(
                    testRun,
                    benchmarkConfig,
                    newHashSet( new Neo4j( params.neo4jCommit(),
                                           params.neo4jVersion().patchVersion(),
                                           params.neo4jEdition(),
                                           params.neo4jBranch(),
                                           params.neo4jBranchOwner() ) ),
                    neo4jConfig,
                    Environment.current(),
                    allResults,
                    tool,
                    java,
                    resultPlans,
                    errorReporter.errors() );

            BenchmarkGroupBenchmarkMetricsPrinter verboseMetricsPrinter = new BenchmarkGroupBenchmarkMetricsPrinter( true );
            System.out.println( verboseMetricsPrinter.toPrettyString( allResults, errorReporter.errors() ) );
            System.out.println( "Exporting results as JSON to: " + resultsJson.toPath().toAbsolutePath() );
            JsonUtil.serializeJson( resultsJson.toPath(), testRunReport );

            Path profilerRecordingsOutputFile = workDir.toPath().resolve( profilerRecordingsOutputDir.toPath() );
            System.out.println( "Copying profiler recordings to: " + profilerRecordingsOutputFile.toAbsolutePath() );
            groupDir.copyProfilerRecordings( profilerRecordingsOutputFile );
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

    public static List<String> argsFor( Path storeDir,
                                        Path neo4jConfigFile,
                                        Path workDir,
                                        Path resultsJson,
                                        Path profilerRecordingsDir,
                                        RunMacroWorkloadParams params )
    {
        List<String> args = Lists.newArrayList(
                "run-workload",
                CMD_DB_PATH,
                storeDir.toAbsolutePath().toString(),
                CMD_NEO4J_CONFIG,
                neo4jConfigFile.toAbsolutePath().toString(),
                CMD_WORK_DIR,
                workDir.toAbsolutePath().toString(),
                CMD_RESULTS_JSON,
                resultsJson.toAbsolutePath().toString(),
                CMD_PROFILER_RECORDINGS_DIR,
                profilerRecordingsDir.toAbsolutePath().toString() );
        args.addAll( params.asArgs() );
        return args;
    }

    private void assertQueryNames( RunMacroWorkloadParams params, Workload workload )
    {
        List<String> allQueryNames = workload.queries().stream().map( Query::name ).collect( toList() );
        List<String> matchedQueries = new ArrayList<>( params.queryNames() );
        matchedQueries.removeAll( allQueryNames );
        if ( !matchedQueries.isEmpty() )
        {
            throw new IllegalArgumentException( format( "%s queries not found in workload %s", matchedQueries, workload.name() ) );
        }
    }
}
