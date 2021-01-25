/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.macro.execution.CountingResultVisitor;
import com.neo4j.bench.macro.workload.ParametersReader;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.QueryString;
import com.neo4j.bench.model.model.Plan;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.util.JsonUtil;

import java.nio.file.Path;
import java.util.Map;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static com.neo4j.bench.common.tool.macro.ExecutionMode.CARDINALITY;
import static com.neo4j.bench.common.tool.macro.ExecutionMode.PLAN;

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
            return getPlan( query, parametersReader, database.inner() );
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
        return createPlan( query.queryString(), parameters, db );
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
                                         : queryString.copyWith( CARDINALITY );
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
        try ( Transaction tx = db.beginTx() )
        {
            CountingResultVisitor resultVisitor = new CountingResultVisitor();
            Result result = tx.execute( queryString.value(), queryParameters );
            result.accept( resultVisitor );
            tx.rollback();
            return result.getExecutionPlanDescription();
        }
    }
}
