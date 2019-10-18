/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.model.Plan;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.macro.execution.CountingResultVisitor;
import com.neo4j.bench.macro.workload.ParametersReader;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.QueryString;

import java.nio.file.Path;
import java.util.Map;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.common.tool.macro.ExecutionMode.PLAN;
import static com.neo4j.bench.common.tool.macro.ExecutionMode.PROFILE;

public class PlanCreator
{
    public static Path exportPlan( ForkDirectory forkDirectory,
                                   Store store,
                                   Edition edition,
                                   Path neo4jConfig,
                                   Query query )
    {
        Plan plan = run( store, edition, neo4jConfig, query );
        Path planFile = forkDirectory.pathForPlan();
        JsonUtil.serializeJson( planFile, plan );
        return planFile;
    }

    private static Plan run( Store store,
                             Edition edition,
                             Path neo4jConfig,
                             Query query )
    {
        try ( EmbeddedDatabase database = EmbeddedDatabase.startWith( store, edition, neo4jConfig ) )
        {
            ParametersReader parametersReader = query.parameters().create();

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
        Map<String,Object> queryParameters = parameters.next();

        // Make sure Planner & Runtime are at default for EXPLAIN run
        QueryString defaultExplainQueryString = queryString
                .copyWith( Planner.DEFAULT )
                .copyWith( Runtime.DEFAULT )
                .copyWith( PLAN );
        ExecutionPlanDescription explainPlanDescription = getPlanDescriptionFor( db, defaultExplainQueryString, queryParameters );

        // For PROFILE run, use the provided Planner & Runtime
        QueryString profileQueryString = (defaultExplainQueryString.isPeriodicCommit())
                                         ? queryString.copyWith( PLAN )
                                         : queryString.copyWith( PROFILE );
        ExecutionPlanDescription profilePlanDescription = getPlanDescriptionFor( db, profileQueryString, queryParameters );

        PlannerDescription plannerDescription = PlannerDescription.fromResults( profilePlanDescription,
                                                                                explainPlanDescription,
                                                                                queryString.planner().name(),
                                                                                queryString.runtime().name() );

        return plannerDescription.toPlan();
    }

    private static ExecutionPlanDescription getPlanDescriptionFor( GraphDatabaseService db,
                                                                   QueryString queryString,
                                                                   Map<String,Object> queryParameters )
    {
        try ( Result result = db.execute( queryString.value(), queryParameters ) )
        {
            CountingResultVisitor resultVisitor = new CountingResultVisitor();
            result.accept( resultVisitor );
            return result.getExecutionPlanDescription();
        }
    }
}
