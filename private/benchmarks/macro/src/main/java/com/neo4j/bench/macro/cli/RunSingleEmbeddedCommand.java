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
import com.google.common.collect.Lists;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.HasPid;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.assist.ProfilerPidMapping;
import com.neo4j.bench.common.profiling.assist.ProfilerPidMappings;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.MeasurementParams;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.QueryRunner;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.options.Edition;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;

import org.neo4j.configuration.connectors.BoltConnector;

import static org.neo4j.configuration.SettingValueParsers.FALSE;

@Command( name = "run-single-embedded", description = "runs one query in a new process for a single workload" )
public class RunSingleEmbeddedCommand implements Runnable
{
    private static final String CMD_WORKLOAD = "--workload";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORKLOAD},
            description = "Path to workload configuration file",
            title = "Workload configuration" )
    @Required
    private String workloadName;

    private static final String CMD_QUERY = "--query";
    @Option( type = OptionType.COMMAND,
            name = {CMD_QUERY},
            description = "Name of query, in the Workload configuration",
            title = "Query name" )
    @Required
    private String queryName;

    private static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DB},
            description = "Store directory matching the selected workload. E.g. 'accesscontrol/' not 'accesscontrol/graph.db/'",
            title = "Store directory" )
    @Required
    private File storeDir;

    private static final String CMD_EDITION = "--db-edition";
    @Option( type = OptionType.COMMAND,
            name = {CMD_EDITION},
            description = "Neo4j edition: COMMUNITY or ENTERPRISE",
            title = "Neo4j edition" )
    private Edition edition = Edition.ENTERPRISE;

    private static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_CONFIG},
            title = "Neo4j configuration file" )
    private File neo4jConfigFile;

    private static final String CMD_OUTPUT = "--output";
    @Option( type = OptionType.COMMAND,
            name = {CMD_OUTPUT},
            description = "Output directory: where result will be written",
            title = "Output directory" )
    @Required
    private File outputDir;

    private static final String CMD_PROFILERS = "--profilers";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PROFILERS},
            description = "Comma separated list of profilers to run with",
            title = "Profilers" )
    private String profilerNames = "";

    private static final String CMD_PLANNER = "--planner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_PLANNER},
            title = "Cypher planner" )
    private Planner planner = Planner.DEFAULT;

    private static final String CMD_RUNTIME = "--runtime";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RUNTIME},
            title = "Cypher runtime" )
    private Runtime runtime = Runtime.DEFAULT;

    private static final String CMD_MODE = "--mode";
    @Option( type = OptionType.COMMAND,
            name = {CMD_MODE},
            description = "Execution mode: EXECUTE (latency), PLAN (latency)",
            title = "Execution mode" )
    private ExecutionMode executionMode = ExecutionMode.EXECUTE;

    @Inject
    private MeasurementParams measurementParams;

    private static final String CMD_JVM_PATH = "--jvm";
    @Option( type = OptionType.COMMAND,
            name = {CMD_JVM_PATH},
            description = "Path to JVM with which this process was launched",
            title = "Path to JVM" )
    @Required
    private File jvmFile;

    private static final String CMD_WORK_DIR = "--work-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORK_DIR},
            description = "Work directory",
            title = "Work directory" )
    @Required
    private File workDir = new File( System.getProperty( "user.dir" ) );

    @Override
    public void run()
    {
        ForkDirectory forkDir = ForkDirectory.openAt( outputDir.toPath() );
        Workload workload;
        try ( Resources resources = new Resources( workDir.toPath() ) )
        {
            workload = Workload.fromName( workloadName, resources, Deployment.embedded() );
        }

        // At this point if it was necessary to copy store (due to mutating query) it should have been done already, trust that store is safe to use
        try ( Store store = Neo4jStore.createFrom( storeDir.toPath(), workload.getDatabaseName() ) )
        {
            if ( neo4jConfigFile != null )
            {
                BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile.toPath() );
            }
            Neo4jConfig neo4jConfig = getNeo4jConfig();
            QueryRunner queryRunner = QueryRunner.queryRunnerFor( executionMode,
                                                                  forkDirectory -> createDatabase( store, edition, neo4jConfig, forkDirectory ) );

            QueryRunner.runSingleCommand( queryRunner,
                                          Jvm.bestEffortOrFail( jvmFile ),
                                          forkDir,
                                          workload,
                                          queryName,
                                          planner,
                                          runtime,
                                          executionMode,
                                          profilerMappings(),
                                          measurementParams.warmupCount(),
                                          measurementParams.minMeasurementSeconds(),
                                          measurementParams.maxMeasurementSeconds(),
                                          measurementParams.measurementCount() );
        }
    }

    Neo4jConfig getNeo4jConfig()
    {
        return Neo4jConfigBuilder.fromFile( neo4jConfigFile )
                                 .withSetting( BoltConnector.enabled, FALSE )
                                 .build();
    }

    private ProfilerPidMappings profilerMappings()
    {
        Pid pid = HasPid.getPid();
        List<InternalProfiler> profilers = createInternalProfilers();
        List<ProfilerPidMapping> mappings = Collections.singletonList( new ProfilerPidMapping( pid, Parameters.NONE, profilers ) );
        return new ProfilerPidMappings( Parameters.NONE, mappings );
    }

    private List<InternalProfiler> createInternalProfilers()
    {
        List<ProfilerType> profilerTypes = ProfilerType.deserializeProfilers( profilerNames );
        ProfilerType.assertInternal( profilerTypes );
        return ProfilerType.createInternalProfilers( profilerTypes );
    }

    static EmbeddedDatabase createDatabase( Store store,
                                            Edition edition,
                                            Neo4jConfig neo4jConfig,
                                            ForkDirectory forkDirectory )
    {
        Path neo4jConfigFile = forkDirectory.create( "neo4j-executing.conf" );
        Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );
        return EmbeddedDatabase.startWith( store, edition, neo4jConfigFile );
    }

    public static List<String> argsFor(
            Query query,
            Store store,
            Edition edition,
            Path neo4jConfig,
            ForkDirectory forkDirectory,
            List<ProfilerType> internalProfilers,
            MeasurementParams measurementParams,
            Jvm jvm,
            Path workDir )
    {
        ArrayList<String> args = Lists.newArrayList(
                "run-single-embedded",
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
                CMD_JVM_PATH,
                jvm.launchJava(),
                CMD_WORK_DIR,
                workDir.toAbsolutePath().toString() );
        args.addAll( measurementParams.asArgs() );
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
