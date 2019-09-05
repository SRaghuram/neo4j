/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.database;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.model.PlanOperator;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.StoreTestUtil;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.PlannerDescription;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PlanDescriptionIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldExtractPlans() throws IOException
    {
        try ( Resources resources = new Resources( temporaryFolder.newFolder().toPath() ) )
        {
            for ( Workload workload : Workload.all( resources, Deployment.embedded() ) )
            {
                System.out.println( "Verifying plan extraction on workload: " + workload.name() );
                Path neo4jConfigFile = temporaryFolder.newFile().toPath();
                try ( Store store = StoreTestUtil.createEmptyStoreFor( workload,
                                                                       temporaryFolder.newFolder().toPath(), /* store */
                                                                       neo4jConfigFile ) )
                {
                    for ( Query query : workload.queries() )
                    {
                        try ( EmbeddedDatabase database = EmbeddedDatabase.startWith( store, Edition.ENTERPRISE, neo4jConfigFile ) )
                        {
                            Result result = database.inner().execute( query.copyWith( ExecutionMode.PLAN ).queryString().value() );
                            result.accept( row -> true );
                            ExecutionPlanDescription rootPlanDescription = result.getExecutionPlanDescription();
                            PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootPlanDescription );
                            String errorMessage = format( "Plans were not equal!\n" +
                                                          "%s %s\n" +
                                                          "%s", workload.name(), query.name(), rootPlanDescription );
                            assertPlansEqual( errorMessage, rootPlanOperator, rootPlanDescription );
                        }
                    }
                }
            }
        }
    }

    private static final String ERROR_IN_TEST_CODE = "Error is probably in test code that traverses logical plan";

    private static void assertPlansEqual( String errorMessage, PlanOperator rootPlanOperator, ExecutionPlanDescription planDescriptionRoot )
    {
        Stack<PlanOperator> planOperators = new Stack<>();
        Stack<ExecutionPlanDescription> planDescriptions = new Stack<>();

        Map<PlanOperator,PlanOperator> parentMap = new HashMap<>();
        Map<PlanOperator,Set<PlanOperator>> childrenMap = new HashMap<>();

        int expectedPlanCount = operatorPlanCountFor( planDescriptionRoot );
        int actualVisitedPlans = 0;

        planOperators.push( rootPlanOperator );
        planDescriptions.push( planDescriptionRoot );

        while ( !planOperators.isEmpty() )
        {
            // plan representations should have the same size
            assertFalse( ERROR_IN_TEST_CODE, planDescriptions.isEmpty() );

            PlanOperator planOperator = planOperators.pop();
            ExecutionPlanDescription planDescription = planDescriptions.pop();

            List<PlanOperator> planOperatorChildren = planOperator.children();
            planOperatorChildren.sort( Comparator.comparing( PlanOperator::id ) );

            List<ExecutionPlanDescription> planDescriptionChildren = Lists.newArrayList( planDescription.getChildren() );
            planDescriptionChildren.sort( Comparator.comparing( PlannerDescription::idFor ) );

            String planDescriptionName = fullNameFor( planDescription );
            String planOperatorName = fullNameFor( planOperator );

            // not yet a leaf operator, continue traversing down the plan
            if ( !childrenMap.containsKey( planOperator ) )
            {
                planOperators.push( planOperator );
                planDescriptions.push( planDescription );

                childrenMap.put( planOperator, Sets.newHashSet( planOperatorChildren ) );
                planOperatorChildren.forEach( child ->
                                              {
                                                  parentMap.put( child, planOperator );
                                                  planOperators.push( child );
                                              } );
                planDescriptionChildren.forEach( planDescriptions::push );
            }
            // not yet a leaf operator, continue traversing down the plan
            // traversing up. last time operator will be seen. compare operator and and continue popping the stack
            else if ( parentMap.containsKey( planOperator ) && /*is not root*/
                      !childrenMap.get( parentMap.get( planOperator ) ).isEmpty() /*has not yet been visited on traversal UP tree*/ )
            {
                assertTrue( ERROR_IN_TEST_CODE + "\n" +
                            "Was not the first time this plan was removed: " + planDescriptionName,
                            childrenMap.get( parentMap.get( planOperator ) ).remove( planOperator ) );

                actualVisitedPlans++;
                assertThat( errorMessage, planOperatorName, equalTo( planDescriptionName ) );
            }
            // this is a leaf operator, compare operators then begin popping the stack
            else
            {
                actualVisitedPlans++;
                assertThat( errorMessage, planOperatorName, equalTo( planDescriptionName ) );
            }
        }
        assertTrue( ERROR_IN_TEST_CODE, planDescriptions.isEmpty() );
        // sanity check, to make sure test code is actually traversing and comparing the entire logical plan
        assertThat( ERROR_IN_TEST_CODE, actualVisitedPlans, equalTo( expectedPlanCount ) );
    }

    private static String fullNameFor( ExecutionPlanDescription planDescription )
    {
        List<String> children = planDescription.getChildren().stream().map( PlanDescriptionIT::nameFor ).sorted().collect( toList() );
        return nameFor( planDescription ) + children;
    }

    private static String nameFor( ExecutionPlanDescription planDescription )
    {
        ArrayList<String> identifiers = Lists.newArrayList( planDescription.getIdentifiers() );
        Collections.sort( identifiers );
        return planDescription instanceof InternalPlanDescription
               ? planDescription.getName() + "[" + PlannerDescription.idFor( planDescription ) + "]" + identifiers
               : planDescription.getName() + identifiers;
    }

    private static String fullNameFor( PlanOperator planOperator )
    {
        List<String> children = planOperator.children().stream().map( PlanDescriptionIT::nameFor ).sorted().collect( toList() );
        return nameFor( planOperator ) + children;
    }

    private static String nameFor( PlanOperator planOperator )
    {
        ArrayList<String> identifiers = Lists.newArrayList( planOperator.identifiers() );
        Collections.sort( identifiers );
        return planOperator.operatorType() + "[" + planOperator.id() + "]" + identifiers;
    }

    private static int operatorPlanCountFor( ExecutionPlanDescription root )
    {
        Stack<ExecutionPlanDescription> plans = new Stack<>();
        int count = 0;
        plans.push( root );
        while ( !plans.isEmpty() )
        {
            count++;
            ExecutionPlanDescription plan = plans.pop();
            plan.getChildren().forEach( plans::push );
        }
        return count;
    }
}
