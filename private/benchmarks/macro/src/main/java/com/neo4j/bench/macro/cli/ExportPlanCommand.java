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
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.macro.execution.database.PlanCreator;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Command( name = "export-plan", description = "exports the plan description for one query of one workload" )
public class ExportPlanCommand implements Runnable
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

    private static final String CMD_RUNTIME = "--runtime";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RUNTIME},
            title = "Cypher runtime" )
    @Required
    private Runtime runtime;

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
    @Required
    private File neo4jConfigFile;

    private static final String CMD_OUTPUT = "--output";
    @Option( type = OptionType.COMMAND,
             name = {CMD_OUTPUT},
             description = "Output directory: where plan will be written",
             title = "Output directory" )
    @Required
    private File outputDir;

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
        ForkDirectory forkDirectory = ForkDirectory.openAt( outputDir.toPath() );
        BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile.toPath() );

        try ( Resources resources = new Resources( workDir.toPath() ) )
        {
            Workload workload = Workload.fromName( workloadName, resources, Deployment.embedded() );
            // At this point if it was necessary to copy store (due to mutating query) it should have been done already, trust that store is safe to use
            try ( Store store = Neo4jStore.createFrom( storeDir.toPath(), workload.getDatabaseName() ) )
            {
                Query query = workload.queryForName( queryName ).copyWith( runtime );
                System.out.println( "Generating plan for : " + query.name() );
                Path planFile = PlanCreator.exportPlan( forkDirectory, store, edition, neo4jConfigFile.toPath(), query );
                System.out.println( "Plan exported to    : " + planFile.toAbsolutePath().toString() );
            }
        }
        catch ( Exception e )
        {
            Path errorFile = forkDirectory.logError( e );
            throw new RuntimeException( "Error exporting plan for query\n" +
                                        "Workload          : " + workloadName + "\n" +
                                        "Query             : " + queryName + "\n" +
                                        "See error file at : " + errorFile.toAbsolutePath().toString(), e );
        }
    }

    public static List<String> argsFor(
            Query query,
            Store store,
            Edition edition,
            Path neo4jConfig,
            ForkDirectory forkDirectory,
            Path workDir )
    {
        ArrayList<String> args = Lists.newArrayList(
                "export-plan",
                CMD_WORKLOAD,
                query.benchmarkGroup().name(),
                CMD_QUERY,
                query.name(),
                CMD_DB,
                store.topLevelDirectory().toAbsolutePath().toString(),
                CMD_EDITION,
                edition.name(),
                CMD_OUTPUT,
                forkDirectory.toAbsolutePath(),
                CMD_WORK_DIR,
                workDir.toAbsolutePath().toString(),
                CMD_RUNTIME,
                query.queryString().runtime().name() );
        if ( null != neo4jConfig )
        {
            args.add( CMD_NEO4J_CONFIG );
            args.add( neo4jConfig.toAbsolutePath().toString() );
        }
        return args;
    }
}
