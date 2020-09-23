/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.database;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.StoreTestUtil;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.PlannerDescription;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.PlanOperator;
import com.neo4j.bench.model.options.Edition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.neo4j.cypher.internal.plandescription.InternalPlanDescription;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class PlannerDescriptionIT
{
    private static final Logger LOG = LoggerFactory.getLogger( PlannerDescriptionIT.class );

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void shouldExtractPlans() throws IOException
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath() ) )
        {
            for ( Workload workload : Workload.all( resources, Deployment.embedded() ) )
            {
                LOG.debug( "Verifying plan extraction on workload: " + workload.name() );
                Path neo4jConfigFile = Files.createTempFile( temporaryFolder.absolutePath(), "neo4j", ".conf" );
                Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfigFile );
                try ( Store store = StoreTestUtil.createTemporaryEmptyStoreFor( workload,
                                                                                Files.createTempDirectory( temporaryFolder.absolutePath(),
                                                                                                     "store" ), /* store */
                                                                                neo4jConfigFile ) )
                {
                    try ( EmbeddedDatabase database = EmbeddedDatabase.startWith( store, Edition.ENTERPRISE, neo4jConfigFile ) )
                    {
                        for ( Query query : workload.queries() )
                        {
                            try ( Transaction tx = database.inner().beginTx() )
                            {
                                Result result = tx.execute( query.copyWith( ExecutionMode.PLAN ).queryString().value() );
                                result.accept( row -> true );
                                ExecutionPlanDescription rootPlanDescription = result.getExecutionPlanDescription();
                                PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootPlanDescription );
                                assertPlansEqual( rootPlanOperator, rootPlanDescription );
                            }
                            catch ( Throwable e )
                            {
                                throw new RuntimeException( format( "Plans comparison failed!\n" +
                                                                    "Workload: %s\n" +
                                                                    "Query:    %s", workload.name(), query.name() ), e );
                            }
                        }
                    }
                }
            }
        }
    }

    private static final String ERROR_IN_TEST_CODE = "Error is probably in test code that traverses logical plan";

    private static void assertPlansEqual( PlanOperator rootPlanOperator, ExecutionPlanDescription planDescriptionRoot )
    {
        Stack<PlanOperator> planOperators = new Stack<>();
        Stack<ExecutionPlanDescription> planDescriptions = new Stack<>();

        Map<PlanOperator,PlanOperator> parentMap = new HashMap<>();
        Map<PlanOperator,Set<PlanOperator>> childrenMap = new HashMap<>();

        int expectedPlanCount = operatorPlanCountFor( planDescriptionRoot, rootPlanOperator );
        int actualVisitedPlans = 0;

        planOperators.push( rootPlanOperator );
        planDescriptions.push( planDescriptionRoot );

        while ( !planOperators.isEmpty() )
        {
            // plan representations should have the same size
            assertFalse( planDescriptions.isEmpty(), ERROR_IN_TEST_CODE );

            PlanOperator planOperator = planOperators.pop();
            ExecutionPlanDescription planDescription = planDescriptions.pop();

            // sort children to ensure both plan representations are traversed in the same (deterministic) order
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
                assertTrue( childrenMap.get( parentMap.get( planOperator ) ).remove( planOperator ),
                            ERROR_IN_TEST_CODE + "\n" +
                            "Was not the first time this plan was removed: " + planDescriptionName );

                actualVisitedPlans++;
                assertThat( planOperatorName, equalTo( planDescriptionName ) );
            }
            // this is a leaf operator, compare operators then begin popping the stack
            else
            {
                actualVisitedPlans++;
                assertThat( planOperatorName, equalTo( planDescriptionName ) );
            }
        }
        // sanity checks, to make sure test code is actually traversing and comparing the entire logical plan
        assertTrue( planDescriptions.isEmpty(), ERROR_IN_TEST_CODE );
        assertThat( ERROR_IN_TEST_CODE, actualVisitedPlans, equalTo( expectedPlanCount ) );
    }

    private static String fullNameFor( ExecutionPlanDescription planDescription )
    {
        List<String> children = planDescription.getChildren().stream().map( PlannerDescriptionIT::nameFor ).sorted().collect( toList() );
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
        List<String> children = planOperator.children().stream().map( PlannerDescriptionIT::nameFor ).sorted().collect( toList() );
        return nameFor( planOperator ) + children;
    }

    private static String nameFor( PlanOperator planOperator )
    {
        ArrayList<String> identifiers = Lists.newArrayList( planOperator.identifiers() );
        Collections.sort( identifiers );
        return planOperator.operatorType() + "[" + planOperator.id() + "]" + identifiers;
    }

    private static int operatorPlanCountFor( ExecutionPlanDescription rootPlanDescription, PlanOperator rootPlanOperator )
    {
        Stack<ExecutionPlanDescription> planDescriptions = new Stack<>();
        int descriptionCount = 0;
        planDescriptions.push( rootPlanDescription );
        while ( !planDescriptions.isEmpty() )
        {
            descriptionCount++;
            ExecutionPlanDescription plan = planDescriptions.pop();
            plan.getChildren().forEach( planDescriptions::push );
        }

        Stack<PlanOperator> planOperators = new Stack<>();
        int operatorCount = 0;
        planOperators.push( rootPlanOperator );
        while ( !planOperators.isEmpty() )
        {
            operatorCount++;
            PlanOperator plan = planOperators.pop();
            plan.children().forEach( planOperators::push );
        }

        assertThat( "Plan representations should be of equal size", descriptionCount, equalTo( operatorCount ) );

        return operatorCount;
    }
}
