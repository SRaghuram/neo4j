/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.submit;

import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.BenchmarkPlan;
import com.neo4j.bench.model.model.Plan;
import com.neo4j.bench.model.model.PlanOperator;
import com.neo4j.bench.model.model.PlanTree;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.common.options.Planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;

import static com.neo4j.bench.model.model.PlanTree.PLAN_DESCRIPTION;
import static java.lang.String.format;

public class PlanTreeSubmitter
{
    private static final String SUBMIT_PLAN_TREE = Resources.fileToString( "/queries/write/submit_plan_tree.cypher" );
    private static final String ATTACH_PLAN_TO_TEST_RUN = Resources.fileToString( "/queries/write/attach_plan_to_test_run.cypher" );

    public static void execute( Transaction tx, TestRun testRun, List<BenchmarkPlan> benchmarkPlans, Planner planner )
    {
        List<BenchmarkPlan> collisionBenchmarkPlans = new ArrayList<>();
        for ( BenchmarkPlan benchmarkPlan : benchmarkPlans )
        {
            Plan plan = benchmarkPlan.plan();
            Map<String,Object> params = new HashMap<>();
            params.put( "plan_description", plan.planTree().asciiPlanDescription() );
            params.put( "plan_description_hash", plan.planTree().hashedPlanDescription() );
            Result statementResult = tx.run( SUBMIT_PLAN_TREE, params );
            Record record = statementResult.next();
            String storedPlanTreeDescription = record
                    .get( "planTree" ).asNode()
                    .get( PLAN_DESCRIPTION )
                    .asString();
            int planTreeNodesCreated = statementResult.consume().counters().nodesCreated();
            switch ( planTreeNodesCreated )
            {
            case 0:
                // a plan tree with the same hash already exists
                if ( !storedPlanTreeDescription.equals( plan.planTree().asciiPlanDescription() ) )
                {
                    // stored description does not match submitted description, possible hash collision
                    collisionBenchmarkPlans.add( benchmarkPlan );
                }
                break;
            case 1:
                // plan tree root was created, now remainder of tree (operators) must be attached to root
                StringBuilder sb = new StringBuilder()
                        .append( "MATCH (planTree:PlanTree)\n" )
                        .append( "WHERE planTree.description_hash=$plan_description_hash AND \n" )
                        .append( "      planTree.description=$plan_description\n" )
                        .append( "CREATE (planTree)-[:HAS_OPERATORS]->" )
                        .append( toTreePattern( plan.planTree(), params ) );
                tx.run( format( "CYPHER planner=%s %s", planner.value(), sb.toString() ), params );
                break;
            default:
                tx.rollback();
                throw new RuntimeException(
                        format( "Submit plan tree query created multiple nodes\n" +
                                " * nodes created: %s\n" +
                                " * plan hash:     %s",
                                planTreeNodesCreated,
                                plan.planTree().hashedPlanDescription() ) );
            }
        }

        for ( BenchmarkPlan benchmarkPlan : benchmarkPlans )
        {
            if ( collisionBenchmarkPlans.contains( benchmarkPlan ) )
            {
                // do not create/attach plans if their plan tree caused hash collision during creation
                // their plan tree was never created, so there is nothing to connect with
                continue;
            }
            Map<String,Object> params = new HashMap<>();
            params.put( "test_run_id", testRun.id() );
            params.put( "benchmark_name", benchmarkPlan.benchmark().name() );
            params.put( "benchmark_group_name", benchmarkPlan.benchmarkGroup().name() );
            params.put( "plan", benchmarkPlan.plan().asMap() );
            params.put( "plan_description_hash", benchmarkPlan.plan().planTree().hashedPlanDescription() );
            tx.run( ATTACH_PLAN_TO_TEST_RUN, params );
        }
        if ( !collisionBenchmarkPlans.isEmpty() )
        {
            tx.rollback();
            throw new RuntimeException( toCollisionsErrorMessage( collisionBenchmarkPlans ) );
        }
    }

    private static String toTreePattern( PlanTree planTree, Map<String,Object> params )
    {
        Namer operatorNamer = new Namer( "operator" );
        Namer propertiesNamer = new Namer( "operatorProperties" );

        StringBuilder sb = new StringBuilder();
        Stack<PlanOperator> planOperatorStack = new Stack<>();
        Map<PlanOperator,String> operatorNames = new HashMap<>();

        // root
        PlanOperator root = planTree.planRoot();
        String rootName = operatorNamer.nextName();
        String rootPropertiesName = propertiesNamer.nextName();
        params.put( rootPropertiesName, root.asMap() );
        sb.append( format( "(%s:Operator $%s)", rootName, rootPropertiesName ) );
        planOperatorStack.push( root );
        operatorNames.put( root, rootName );

        // children
        while ( !planOperatorStack.isEmpty() )
        {
            PlanOperator current = planOperatorStack.pop();
            String currentOperatorName = operatorNames.get( current );

            current.children().forEach( child ->
                                        {
                                            String childOperatorName = operatorNamer.nextName();
                                            String childPropertiesName = propertiesNamer.nextName();

                                            params.put( childPropertiesName, child.asMap() );
                                            sb.append( format( ",\n(%s)-[:HAS_CHILD]->(%s:Operator $%s)",
                                                               currentOperatorName, childOperatorName, childPropertiesName ) );

                                            planOperatorStack.push( child );
                                            operatorNames.put( child, childOperatorName );
                                        }
            );
        }

        return sb.toString();
    }

    private static String toCollisionsErrorMessage( List<BenchmarkPlan> collisionBenchmarkPlans )
    {
        StringBuilder sb = new StringBuilder( "Due to hash collision the following plans were not created:\n" );
        collisionBenchmarkPlans.stream().map( PlanTreeSubmitter::toBenchmarkPlanString ).forEach( sb::append );
        return sb.toString();
    }

    private static String toBenchmarkPlanString( BenchmarkPlan benchmarkPlan )
    {
        return " * " +
               benchmarkPlan.benchmarkGroup().name() + "." +
               benchmarkPlan.benchmark().name() + " , " +
               PlanTree.PLAN_DESCRIPTION_HASH + " = " +
               benchmarkPlan.plan().planTree().hashedPlanDescription();
    }

    private static class Namer
    {
        private final String namePrefix;
        private int count;

        private Namer( String namePrefix )
        {
            this.namePrefix = namePrefix;
        }

        private String nextName()
        {
            return namePrefix + count++;
        }
    }
}
