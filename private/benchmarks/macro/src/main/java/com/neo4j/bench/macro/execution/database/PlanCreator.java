/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Plan;
import com.neo4j.bench.client.model.PlanCompilationMetrics;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.client.profiling.PlannerDescription;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.JsonUtil;
import com.neo4j.bench.macro.execution.CountingResultVisitor;
import com.neo4j.bench.macro.workload.ParametersReader;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.QueryString;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.macro.execution.Options.ExecutionMode.PLAN;
import static com.neo4j.bench.macro.execution.Options.ExecutionMode.PROFILE;

public class PlanCreator
{
    private static final PlanCompilationMetrics NO_PLAN_COMPILATION_METRICS = new PlanCompilationMetrics( new HashMap<>() );

    public static Path exportPlan( ForkDirectory forkDirectory,
                                   Store store,
                                   Edition edition,
                                   Path neo4jConfig,
                                   Query query )
    {
        Plan plan = run( store, edition, neo4jConfig, query, forkDirectory );
        Path planFile = forkDirectory.pathForPlan();
        JsonUtil.serializeJson( planFile, plan );
        return planFile;
    }

    private static Plan run( Store store,
                             Edition edition,
                             Path neo4jConfig,
                             Query query,
                             ForkDirectory forkDirectory )
    {
        try ( Database database = Database.startWith( store, edition, neo4jConfig ) )
        {
            ParametersReader parametersReader = query.parameters().create( forkDirectory );

            /*
             * Run query with profile to get plan & output query plan
             */
            return getPlan( query, parametersReader, database.db() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving plan\n" + query, e );
        }
    }

    private static Plan getPlan( Query query,
                                 ParametersReader parameters,
                                 GraphDatabaseService db ) throws Exception
    {
        if ( query.queryString().isPeriodicCommit() )
        {
            return createPlan( query.queryString(), parameters, db );
        }
        else
        {
            try ( Transaction tx = db.beginTx() )
            {
                return createPlan( query.queryString(), parameters, db );
                // Always roll back, mutating the store prevents later forks from reusing it.
            }
        }
    }

    private static Plan createPlan( QueryString queryString,
                                    ParametersReader parameters,
                                    GraphDatabaseService db ) throws Exception
    {
        CountingResultVisitor resultVisitor = new CountingResultVisitor();

        Map<String,Object> queryParameters = parameters.next();

        // Make sure Planner & Runtime are at default for EXPLAIN run
        QueryString defaultExplainQueryString = queryString
                .copyWith( Planner.DEFAULT )
                .copyWith( Runtime.DEFAULT )
                .copyWith( PLAN );
        Result explainResult = db.execute( defaultExplainQueryString.value(), queryParameters );
        explainResult.accept( resultVisitor );

        // For PROFILE run, use the provided Planner & Runtime
        QueryString profileQueryString = (defaultExplainQueryString.isPeriodicCommit())
                                         ? queryString.copyWith( PLAN )
                                         : queryString.copyWith( PROFILE );
        Result profileResult = db.execute( profileQueryString.value(), queryParameters );
        profileResult.accept( resultVisitor );

        PlannerDescription plannerDescription = PlannerDescription.fromResults( profileResult,
                                                                                explainResult,
                                                                                queryString.planner().name(),
                                                                                queryString.runtime().name() );

        return plannerDescription.toPlan( NO_PLAN_COMPILATION_METRICS );
    }
}
