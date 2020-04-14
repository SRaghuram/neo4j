/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher.internal.javacompat;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.extension.Inject;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsNull.notNullValue;

@EnterpriseDbmsExtension
class ExecutionResultTest
{
    private static final String CURRENT_VERSION = "CYPHER 4.1";
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldBePossibleToConsumeCompiledExecutionResultsWithIterator()
    {
        // Given
        createNode();
        createNode();

        try ( Transaction transaction = db.beginTx() )
        {
            // When
            List<Map<String,Object>> listResult;
            try ( Result result = transaction.execute( "CYPHER runtime=legacy_compiled MATCH (n) RETURN n" ) )
            {
                listResult = Iterators.asList( result );
            }

            // Then
            assertThat( listResult, hasSize( 2 ) );
            transaction.commit();
        }
    }

    @Test
    void shouldBePossibleToCloseNotFullyConsumedCompiledExecutionResults()
    {
        // Given
        createNode();
        createNode();

        try ( Transaction transaction = db.beginTx() )
        {
            // When
            Map<String,Object> firstRow = null;
            try ( Result result = transaction.execute( "CYPHER runtime=legacy_compiled MATCH (n) RETURN n" ) )
            {
                if ( result.hasNext() )
                {
                    firstRow = result.next();
                }
            }

            // Then
            assertThat( firstRow, notNullValue() );
            transaction.commit();
        }
    }

    @Test
    void shouldBePossibleToConsumeCompiledExecutionResultsWithVisitor()
    {
        // Given
        createNode();
        createNode();

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            final List<Result.ResultRow> listResult = new ArrayList<>();
            try ( Result result = transaction.execute( "CYPHER runtime=legacy_compiled MATCH (n) RETURN n" ) )
            {
                result.accept( row ->
                {
                    listResult.add( row );
                    return true;
                } );
            }

            // Then
            assertThat( listResult, hasSize( 2 ) );
            transaction.commit();
        }
    }

    @Test
    void shouldBePossibleToCloseNotFullyVisitedExecutionResult()
    {
        // Given
        createNode();
        createNode();

        // When
        for ( String runtime : asList( "INTERPRETED", "SLOTTED", "LEGACY_COMPILED", "PIPELINED" ) )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                final List<Result.ResultRow> listResult = new ArrayList<>();
                try ( Result result = transaction.execute( String.format( "CYPHER runtime=%s MATCH (n) RETURN n", runtime ) ) )
                {
                    result.accept( row ->
                    {
                        listResult.add( row );
                        // return false so that no more result rows would be visited
                        return false;
                    } );
                }

                // Then
                assertThat( listResult, hasSize( 1 ) );
                transaction.commit();
            }
        }
    }

    @Test
    void shouldBePossibleToCloseNotFullyVisitedReadWriteExecutionResult()
    {
        // Given
        createNode();
        createNode();

        // When
        for ( String runtime : asList( "INTERPRETED", "SLOTTED" ) )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                final List<Result.ResultRow> listResult = new ArrayList<>();
                //This is really a read query but the planner will think of it as a read-write and force it to be
                //materialized
                try ( Result result = transaction.execute( String.format( "CYPHER runtime=%s MERGE (n) RETURN n", runtime ) ) )
                {
                    result.accept( row ->
                    {
                        listResult.add( row );
                        // return false so that no more result rows would be visited
                        return false;
                    } );
                }

                // Then
                assertThat( listResult, hasSize( 1 ) );
                transaction.commit();
            }
        }
    }

    @Test
    void shouldBePossibleToCloseFullyVisitedExecutionResult()
    {
        // Given
        createNode();
        createNode();

        // When
        for ( String runtime : asList( "INTERPRETED", "SLOTTED", "LEGACY_COMPILED", "PIPELINED" ) )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                final List<Result.ResultRow> listResult = new ArrayList<>();
                try ( Result result = transaction.execute( String.format( "CYPHER runtime=%s MATCH (n) RETURN n", runtime ) ) )
                {
                    result.accept( row ->
                    {
                        listResult.add( row );
                        // return false so that no more result rows would be visited
                        return true;
                    } );
                }

                // Then
                assertThat( listResult, hasSize( 2 ) );
                transaction.commit();
            }
        }
    }

    @Test
    void shouldBePossibleToCloseNotConsumedCompiledExecutionResult()
    {
        // Given
        createNode();

        try ( Transaction transaction = db.beginTx() )
        {
            // Then
            // just close result without consuming it
            transaction.execute( "CYPHER runtime=legacy_compiled MATCH (n) RETURN n" ).close();
            transaction.commit();
        }
    }

    @Test
    void shouldCreateAndDropUniqueConstraints()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Result create = transaction.execute( "CREATE CONSTRAINT ON (n:L) ASSERT n.prop IS UNIQUE" );
            Result drop = transaction.execute( "DROP CONSTRAINT ON (n:L) ASSERT n.prop IS UNIQUE" );

            assertThat( create.getQueryStatistics().getConstraintsAdded(), equalTo( 1 ) );
            assertThat( create.getQueryStatistics().getConstraintsRemoved(), equalTo( 0 ) );
            assertThat( drop.getQueryStatistics().getConstraintsAdded(), equalTo( 0 ) );
            assertThat( drop.getQueryStatistics().getConstraintsRemoved(), equalTo( 1 ) );
            transaction.commit();
        }
    }

    @Test
    void shouldCreateAndDropExistenceConstraints()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Result create = transaction.execute( "CREATE CONSTRAINT ON (n:L) ASSERT exists(n.prop)" );
            Result drop = transaction.execute( "DROP CONSTRAINT ON (n:L) ASSERT exists(n.prop)" );

            assertThat( create.getQueryStatistics().getConstraintsAdded(), equalTo( 1 ) );
            assertThat( create.getQueryStatistics().getConstraintsRemoved(), equalTo( 0 ) );
            assertThat( drop.getQueryStatistics().getConstraintsAdded(), equalTo( 0 ) );
            assertThat( drop.getQueryStatistics().getConstraintsRemoved(), equalTo( 1 ) );
            transaction.commit();
        }
    }

    @Test
    void shouldCreateAndDropConstraintsWithName()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Result create = transaction.execute( "CREATE CONSTRAINT my_constraint ON (n:L) ASSERT exists(n.prop)" );
            Result drop = transaction.execute( "DROP CONSTRAINT my_constraint" );

            assertThat( create.getQueryStatistics().getConstraintsAdded(), equalTo( 1 ) );
            assertThat( create.getQueryStatistics().getConstraintsRemoved(), equalTo( 0 ) );
            assertThat( drop.getQueryStatistics().getConstraintsAdded(), equalTo( 0 ) );
            assertThat( drop.getQueryStatistics().getConstraintsRemoved(), equalTo( 1 ) );
            transaction.commit();
        }
    }

    @Test
    void shouldShowRuntimeInExecutionPlanDescription()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Result result = transaction.execute( "EXPLAIN MATCH (n) RETURN n.prop" );

            // When
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

            // Then
            assertThat( arguments.get( "version" ), equalTo( CURRENT_VERSION ) );
            assertThat( arguments.get( "planner" ), equalTo( "COST" ) );
            assertThat( arguments.get( "planner-impl" ), equalTo( "IDP" ) );
            assertThat( arguments.get( "runtime" ), notNullValue() );
            assertThat( arguments.get( "runtime-impl" ), notNullValue() );
            transaction.commit();
        }
    }

    @Test
    void shouldShowCompiledRuntimeInExecutionPlan()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Result result = transaction.execute( "EXPLAIN CYPHER runtime=legacy_compiled MATCH (n) RETURN n.prop" );

            // When
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

            // Then
            assertThat( arguments.get( "version" ), equalTo( CURRENT_VERSION ) );
            assertThat( arguments.get( "planner" ), equalTo( "COST" ) );
            assertThat( arguments.get( "planner-impl" ), equalTo( "IDP" ) );
            assertThat( arguments.get( "runtime" ), equalTo( "LEGACY_COMPILED" ) );
            assertThat( arguments.get( "runtime-impl" ), equalTo( "LEGACY_COMPILED" ) );
            transaction.commit();
        }
    }

    @Test
    void shouldShowInterpretedRuntimeInExecutionPlan()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Result result = transaction.execute( "EXPLAIN CYPHER runtime=interpreted MATCH (n) RETURN n.prop" );

            // When
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

            // Then
            assertThat( arguments.get( "version" ), equalTo( CURRENT_VERSION ) );
            assertThat( arguments.get( "planner" ), equalTo( "COST" ) );
            assertThat( arguments.get( "planner-impl" ), equalTo( "IDP" ) );
            assertThat( arguments.get( "runtime" ), equalTo( "INTERPRETED" ) );
            assertThat( arguments.get( "runtime-impl" ), equalTo( "INTERPRETED" ) );
            transaction.commit();
        }
    }

    @Test
    void shouldShowArgumentsExecutionPlan()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Result result = transaction.execute( "EXPLAIN CALL db.labels()" );

            // When
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

            // Then
            assertThat( arguments.get( "version" ), equalTo( CURRENT_VERSION ) );
            assertThat( arguments.get( "planner" ), equalTo( "COST" ) );
            assertThat( arguments.get( "planner-impl" ), equalTo( "IDP" ) );
            assertThat( arguments.get( "runtime" ), equalTo( "PIPELINED" ) );
            assertThat( arguments.get( "runtime-impl" ), equalTo( "PIPELINED" ) );
            transaction.commit();
        }
    }

    @Test
    void shouldShowArgumentsInProfileExecutionPlan()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Result result = transaction.execute( "PROFILE CALL db.labels()" );
            result.resultAsString();

            // When
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

            // Then
            assertThat( arguments.get( "version" ), equalTo( CURRENT_VERSION ) );
            assertThat( arguments.get( "planner" ), equalTo( "COST" ) );
            assertThat( arguments.get( "planner-impl" ), equalTo( "IDP" ) );
            assertThat( arguments.get( "runtime" ), equalTo( "PIPELINED" ) );
            assertThat( arguments.get( "runtime-impl" ), equalTo( "PIPELINED" ) );
            transaction.commit();
        }
    }

    @Test
    void shouldShowArgumentsInSchemaExecutionPlan()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Result result = transaction.execute( "EXPLAIN CREATE INDEX FOR (n:L) ON (n.prop)" );

            // When
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

            // Then
            assertThat( arguments.get( "version" ), equalTo( CURRENT_VERSION ) );
            assertThat( arguments.get( "planner" ), equalTo( "ADMINISTRATION" ) );
            assertThat( arguments.get( "planner-impl" ), equalTo( "ADMINISTRATION" ) );
            assertThat( arguments.get( "runtime" ), equalTo( "SCHEMA" ) );
            assertThat( arguments.get( "runtime-impl" ), equalTo( "SCHEMA" ) );
            assertThat( arguments.get( "IndexName" ), equalTo( null ) );
            transaction.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Result result = transaction.execute( "EXPLAIN CREATE INDEX my_index FOR (n:L) ON (n.prop)" );

            // When
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

            // Then
            assertThat( arguments.get( "version" ), equalTo( CURRENT_VERSION ) );
            assertThat( arguments.get( "planner" ), equalTo( "ADMINISTRATION" ) );
            assertThat( arguments.get( "planner-impl" ), equalTo( "ADMINISTRATION" ) );
            assertThat( arguments.get( "runtime" ), equalTo( "SCHEMA" ) );
            assertThat( arguments.get( "runtime-impl" ), equalTo( "SCHEMA" ) );
            assertThat( arguments.get( "IndexName" ), equalTo( "my_index" ) );
            transaction.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Result result = transaction.execute( "EXPLAIN CREATE CONSTRAINT my_constraint ON (n:L) ASSERT EXISTS (n.prop)" );

            // When
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();

            // Then
            assertThat( arguments.get( "version" ), equalTo( CURRENT_VERSION ) );
            assertThat( arguments.get( "planner" ), equalTo( "ADMINISTRATION" ) );
            assertThat( arguments.get( "planner-impl" ), equalTo( "ADMINISTRATION" ) );
            assertThat( arguments.get( "runtime" ), equalTo( "SCHEMA" ) );
            assertThat( arguments.get( "runtime-impl" ), equalTo( "SCHEMA" ) );
            assertThat( arguments.get( "ConstraintName" ), equalTo( "my_constraint" ) );
            transaction.commit();
        }
    }

    @Test
    void shouldReturnListFromSplit()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Object s = transaction.execute( "RETURN split('hello, world', ',') AS s" ).next().get( "s" );
            assertThat( s, instanceOf( List.class ) );
        }
    }

    @Test
    void shouldReturnCorrectArrayType()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            transaction.execute( "CREATE (p:Person {names:['adsf', 'adf' ]})" );

            // When
            Object result = transaction.execute( "MATCH (n) RETURN n.names" ).next().get( "n.names" );

            // Then
            assertThat( result, CoreMatchers.instanceOf( String[].class ) );
            transaction.commit();
        }
    }

    @Test
    void shouldContainCompletePlanFromFromLegacyVersions()
    {
        for ( String version : new String[]{"3.5", "4.0"} )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                // Given
                Result result = transaction.execute( String.format( "EXPLAIN CYPHER %s MATCH (n) RETURN n", version ) );

                // When
                ExecutionPlanDescription description = result.getExecutionPlanDescription();

                // Then
                assertThat( description.getName(), equalTo( "ProduceResults" ) );
                assertThat( description.getChildren().get( 0 ).getName(), equalTo( "AllNodesScan" ) );
                transaction.commit();
            }
        }
    }

    @Test
    void shouldContainCompleteProfileFromFromLegacyVersions()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();

            tx.commit();
        }

        for ( String version : new String[]{"3.5", "4.0"} )
        {
            // When
            try ( Transaction transaction = db.beginTx() )
            {
                Result result = transaction.execute( String.format( "PROFILE CYPHER %s MATCH (n) RETURN n", version ) );
                result.resultAsString();
                ExecutionPlanDescription.ProfilerStatistics stats = result.getExecutionPlanDescription()//ProduceResult
                        .getChildren().get( 0 ) //AllNodesScan
                        .getProfilerStatistics();

                // Then
                assertThat( "Mismatching db-hits for version " + version, stats.getDbHits(), equalTo( 2L ) );
                assertThat( "Mismatching rows for version " + version, stats.getRows(), equalTo( 1L ) );

                if ( stats.hasPageCacheStats() )
                {
                    assertThat( "Mismatching page cache hits for version " + version, stats.getPageCacheHits(), greaterThanOrEqualTo( 0L ) );
                    assertThat( "Mismatching page cache misses for version " + version, stats.getPageCacheMisses(), greaterThanOrEqualTo( 0L ) );
                    assertThat( "Mismatching page cache hit ratio for version " + version, stats.getPageCacheHitRatio(), greaterThanOrEqualTo( 0.0 ) );
                }
                transaction.commit();
            }
        }
    }

    private void createNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }
    }
}
