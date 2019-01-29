package com.neo4j.bench.macro.cli;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.client.profiling.InternalProfiler;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.macro.execution.QueryRunner;
import com.neo4j.bench.macro.execution.Options.ExecutionMode;
import com.neo4j.bench.macro.execution.database.PlanCreator;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.and;
import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.ofCount;
import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.ofDuration;
import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.or;

import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;

@Command( name = "run-single", description = "runs one query in a new process for a single workload" )
public class RunSingleCommand implements Runnable
{
    private static final String CMD_WORKLOAD = "--workload";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORKLOAD},
            description = "Path to workload configuration file",
            title = "Workload configuration",
            required = true )
    private String workloadName;

    private static final String CMD_QUERY = "--query";
    @Option( type = OptionType.COMMAND,
            name = {CMD_QUERY},
            description = "Name of query, in the Workload configuration",
            title = "Query name",
            required = true )
    private String queryName;

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
    private Edition edition = Edition.ENTERPRISE;

    private static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_CONFIG},
            title = "Neo4j configuration file",
            required = false )
    private File neo4jConfigFile;

    private static final String CMD_OUTPUT = "--output";
    @Option( type = OptionType.COMMAND,
            name = {CMD_OUTPUT},
            description = "Output directory: where result will be written",
            title = "Output directory",
            required = true )
    private File outputDir;

    private static final String CMD_PROFILERS = "--profilers";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILERS},
            description = "Comma separated list of profilers to run with",
            title = "Profilers",
            required = false )
    private String profilerNames = "";

    private static final String CMD_PLANNER = "--planner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PLANNER},
            title = "Cypher planner",
            required = false )
    private Planner planner = Planner.DEFAULT;

    private static final String CMD_RUNTIME = "--runtime";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RUNTIME},
            title = "Cypher runtime",
            required = false )
    private Runtime runtime = Runtime.DEFAULT;

    private static final String CMD_MODE = "--mode";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MODE},
            description = "Execution mode: EXECUTE (latency), PLAN (latency)",
            title = "Execution mode",
            required = false )
    private ExecutionMode executionMode = ExecutionMode.EXECUTE;

    private static final String CMD_WARMUP_COUNT = "--warmup-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WARMUP_COUNT},
            title = "Warmup execution count",
            required = true )
    private int warmupCount;

    private static final String CMD_MEASUREMENT_COUNT = "--measurement-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MEASUREMENT_COUNT},
            title = "Measurement execution count",
            required = true )
    private int measurementCount;

    // TODO add to run workload?
    private static final String CMD_MIN_MEASUREMENT_SECONDS = "--min-measurement-seconds";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MIN_MEASUREMENT_SECONDS},
            title = "Min measurement execution duration, in seconds",
            required = false )
    private int minMeasurementSeconds = 30; // 30 seconds

    // TODO add to run workload?
    private static final String CMD_MAX_MEASUREMENT_SECONDS = "--max-measurement-seconds";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MAX_MEASUREMENT_SECONDS},
            title = "Max measurement execution duration, in seconds",
            required = false )
    private int maxMeasurementSeconds = 10 * 60; // 10 minutes

    private static final String CMD_EXPORT_PLAN = "--export-plan";
    @Option( type = OptionType.COMMAND,
            name = {CMD_EXPORT_PLAN},
            title = "Specifies if a logical plan should be exported",
            required = false )
    private boolean doExportPlan;

    private static final String CMD_JVM_PATH = "--jvm";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_PATH},
            description = "Path to JVM with which this process was launched",
            title = "Path to JVM",
            required = true )
    private File jvmFile;

    @Override
    public void run()
    {
        Jvm jvm = Jvm.bestEffortOrFail( jvmFile );

        ForkDirectory forkDir = ForkDirectory.openAt( outputDir.toPath() );

        // At this point if it was necessary to copy store (due to mutating query) it should have been done already, trust that store is safe to use
        try ( Store store = Store.createFrom( storeDir.toPath() );
              Resources resources = new Resources() )
        {
            Workload workload = Workload.fromName( workloadName, resources );
            Query query = workload.queryForName( queryName )
                                  .copyWith( planner )
                                  .copyWith( runtime )
                                  .copyWith( executionMode );
            List<ProfilerType> profilerTypes = ProfilerType.deserializeProfilers( profilerNames );

            List<ProfilerType> internalProfilerTypes = profilerTypes.stream().filter( ProfilerType::isInternal ).collect( toList() );
            profilerTypes.removeAll( internalProfilerTypes );
            if ( !profilerTypes.isEmpty() )
            {
                throw new RuntimeException( "Command only supports 'internal' profilers\n" +
                                            "But received the following non-internal profilers : " + profilerTypes + "\n" +
                                            "Complete list of valid internal profilers         : " + ProfilerType.internalProfilers() + "\n" +
                                            "Complete list of valid external profilers         : " + ProfilerType.externalProfilers() );
            }

            List<InternalProfiler> internalProfilers = internalProfilerTypes.stream()
                                                                            .map( ProfilerType::create )
                                                                            .map( profiler -> (InternalProfiler) profiler )
                                                                            .collect( toList() );

            if ( neo4jConfigFile != null )
            {
                BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile.toPath() );
            }
            Neo4jConfig neo4jConfig = Neo4jConfig.fromFile( neo4jConfigFile );

            if ( doExportPlan )
            {
                System.out.println( "Generating plan for : " + query.name() );
                Path planFile = PlanCreator.exportPlan( forkDir, store, edition, null == neo4jConfigFile ? null : neo4jConfigFile.toPath(), query );
                System.out.println( "Plan exported to    : " + planFile.toAbsolutePath().toString() );
            }

            QueryRunner queryRunner = QueryRunner.runnerFor( executionMode );
            queryRunner.run( jvm,
                             store,
                             edition,
                             neo4jConfig,
                             internalProfilers,
                             query,
                             forkDir,
                             measurementControlFor( warmupCount ),
                             measurementControlFor( measurementCount ) );

            /*
            This command does not export summary of results as JSON. This is because it has two purposes and neither of them requires it:
              (1) Interactive execution, locally by developers. Result logs and console output should suffice.
              (2) Invoked from 'run-workload'. Multiple forks for same query, so result log merging needs to happen before constructing summary.
            */
        }
        catch ( Exception e )
        {
            Path errorFile = forkDir.logError( e );
            throw new RuntimeException( "Error running query\n" +
                                        "Workload          : " + workloadName + "\n" +
                                        "Query             : " + queryName + "\n" +
                                        "See error file at : " + errorFile.toAbsolutePath().toString(), e );
        }
    }

    private MeasurementControl measurementControlFor( int minCount )
    {
        return and( // minimum duration
                    ofDuration( ofSeconds( minMeasurementSeconds ) ),

                    // count or maximum duration, whichever happens first
                    or( // minimum number of query executions
                        ofCount( minCount ),
                        // maximum duration
                        ofDuration( ofSeconds( maxMeasurementSeconds ) ) ) );
    }

    public static List<String> argsFor(
            Query query,
            Store store,
            Edition edition,
            Path neo4jConfig,
            ForkDirectory forkDirectory,
            List<ProfilerType> internalProfilers,
            int warmupCount,
            int measurementCount,
            boolean doExportPlan,
            Jvm jvm )
    {
        ArrayList<String> args = Lists.newArrayList(
                "run-single",
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
                CMD_DB,
                store.topLevelDirectory().toAbsolutePath().toString(),
                CMD_EDITION,
                edition.name(),
                CMD_OUTPUT,
                forkDirectory.toAbsolutePath(),
                CMD_WARMUP_COUNT,
                Integer.toString( warmupCount ),
                CMD_MEASUREMENT_COUNT,
                Integer.toString( measurementCount ),
                CMD_JVM_PATH,
                jvm.launchJava() );
        if ( doExportPlan )
        {
            args.add( CMD_EXPORT_PLAN );
        }
        if ( !internalProfilers.isEmpty() )
        {
            args.add( CMD_PROFILERS );
            args.add( ProfilerType.serializeProfilers( internalProfilers ) );
        }
        if ( null != neo4jConfig )
        {
            args.add( CMD_NEO4J_CONFIG );
            args.add( neo4jConfig.toAbsolutePath().toString() );
        }
        return args;
    }
}
