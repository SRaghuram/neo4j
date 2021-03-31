/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.cli;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.common.ParameterVerifier;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.tool.macro.BuildParams;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.MeasurementParams;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;
import javax.inject.Inject;

import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_EDITION;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_EXECUTION_MODE;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_FORKS;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_JVM_ARGS;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_JVM_PATH;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_NEO4J_DEPLOYMENT;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_PLANNER;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_PROFILERS;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_RECREATE_SCHEMA;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_RUNTIME;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_SKIP_FLAMEGRAPHS;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_TIME_UNIT;
import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_WORKLOAD;
import static java.lang.String.format;

public abstract class BaseRunWorkloadCommand implements Runnable
{

    private static final Logger LOG = LoggerFactory.getLogger( BaseRunWorkloadCommand.class );

    @Option( type = OptionType.COMMAND,
             name = {CMD_WORKLOAD},
             description = "Name of workload to run",
             title = "Workload" )
    @Required
    private String workloadName;

    @Option( type = OptionType.COMMAND,
             name = {CMD_EDITION},
             description = "Neo4j edition: COMMUNITY or ENTERPRISE",
             title = "Neo4j edition" )
    private Edition neo4jEdition = Edition.ENTERPRISE;

    @Option( type = OptionType.COMMAND,
             name = {CMD_JVM_PATH},
             description = "Path to JVM -- will also be used when launching fork processes",
             title = "Path to JVM" )
    @Required
    private File jvmFile;

    @Option( type = OptionType.COMMAND,
             name = {CMD_PROFILERS},
             description = "Comma separated list of profilers to run with",
             title = "Profilers" )
    @Required
    private String profilerNames;

    @Inject
    private MeasurementParams measurementParams;

    @Option( type = OptionType.COMMAND,
             name = {CMD_FORKS},
             title = "Fork count" )
    private int measurementForkCount = 1;

    @Option( type = OptionType.COMMAND,
             name = {CMD_TIME_UNIT},
             description = "Time unit to report results in",
             title = "Time unit" )
    private TimeUnit unit = TimeUnit.MICROSECONDS;

    @Option( type = OptionType.COMMAND,
             name = {CMD_RUNTIME},
             description = "Cypher runtime",
             title = "Cypher runtime" )
    private Runtime runtime = Runtime.DEFAULT;

    @Option( type = OptionType.COMMAND,
             name = {CMD_PLANNER},
             description = "Cypher planner",
             title = "Cypher planner" )
    private Planner planner = Planner.DEFAULT;

    @Option( type = OptionType.COMMAND,
             name = {CMD_EXECUTION_MODE},
             description = "How to execute: run VS plan",
             title = "Run vs Plan" )
    @Required
    private ExecutionMode executionMode;

    @Option( type = OptionType.COMMAND,
             name = {CMD_JVM_ARGS},
             description = "JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC -Xms4g -Xmx4g')",
             title = "JVM Args" )
    @Required
    private String jvmArgs;

    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_DEPLOYMENT},
             description = "Valid values: 'embedded' or 'server:<path_to_neo4j_server>'",
             title = "Deployment mode" )
    @Required
    private String deploymentMode;

    @Option( type = OptionType.COMMAND,
             name = {CMD_RECREATE_SCHEMA},
             description = "Drop indexes and constraints, delete index directories (and transaction logs), then recreate indexes and constraints",
             title = "Recreate Schema" )
    private boolean recreateSchema;

    @Option( type = OptionType.COMMAND,
             name = {CMD_SKIP_FLAMEGRAPHS},
             description = "Skip FlameGraph generation",
             title = "Skip FlameGraph generation" )
    private boolean skipFlameGraphs;

    // -----------------------------------------------------------------------
    // Result Client Report Results Args
    // -----------------------------------------------------------------------

    @Inject
    private BuildParams buildParams;

    @Option( type = OptionType.COMMAND,
             name = {RunMacroWorkloadParams.CMD_QUERIES},
             description = "Comma separated list of queries to be run, if absent all queries will be run",
             title = "Query name" )
    private String queryNames = "";

    @Override
    public final void run()
    {

        try
        {
            LogManager.getLogManager().readConfiguration( BaseRunWorkloadCommand.class.getResourceAsStream( "/bench/logging.properties" ) );
        }
        catch ( SecurityException | IOException e )
        {
            LOG.debug( format( "failed to initialize java.util.logging\n %s", e ) );
        }

        Deployment deployment = Deployment.parse( deploymentMode );
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.parse( profilerNames );
        JvmArgs jvmArgs = JvmArgs.parse( this.jvmArgs );
        buildParams = buildParams.teamcityBranchAsRealBranch();
        ParameterVerifier.performSanityChecks( buildParams.neo4jBranchOwner(),
                                               buildParams.neo4jVersion().fullVersion(),
                                               buildParams.neo4jBranch(),
                                               buildParams.triggeredBy() );
        List<String> queries = StringUtils.isBlank( queryNames )
                               ? Collections.emptyList()
                               : Arrays.asList( queryNames.split( "," ) );

        RunMacroWorkloadParams commandParams = new RunMacroWorkloadParams( workloadName,
                                                                           queries,
                                                                           neo4jEdition,
                                                                           jvmFile.toPath(),
                                                                           profilers,
                                                                           measurementParams,
                                                                           measurementForkCount,
                                                                           unit,
                                                                           runtime,
                                                                           planner,
                                                                           executionMode,
                                                                           jvmArgs,
                                                                           recreateSchema,
                                                                           skipFlameGraphs,
                                                                           deployment,
                                                                           buildParams );

        doRun( commandParams );
    }

    protected abstract void doRun( RunMacroWorkloadParams params );
}
