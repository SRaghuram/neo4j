/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher.internal.javacompat;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.EnterpriseCompilerFactory;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.anyOf;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class ExpressionEngineConfigurationTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldBeJitCompileOnSecondAccessByDefault()
    {
        // Given
        String query = "RETURN sin(cos(sin(cos(rand()))))";
        GraphDatabaseService db = withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.DEFAULT, 1 );

        assertNotUsingCompiled( db, query );

        assertUsingCompiled( db, query );
    }

    @Test
    void shouldNotUseCompiledExpressionsFirstTimeWithJitEnabled()
    {
        assertNotUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.ONLY_WHEN_HOT, 1 ), "RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsFirstTimeWhenLimitIsZero()
    {
        assertUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.ONLY_WHEN_HOT, 0 ), "RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsWhenQueryIsHotWithJitEnabled()
    {
        // Given
        String query = "RETURN sin(cos(sin(cos(rand()))))";
        GraphDatabaseService db = withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.ONLY_WHEN_HOT, 3 );

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( query );
            transaction.execute( query );
            transaction.execute( query );
        }

        // Then
        assertUsingCompiled( db, query );
    }

    @Test
    void shouldUseCompiledExpressionsFirstTimeWhenConfigured()
    {
        assertUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.COMPILED, 42 ), "RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsFirstTimeWhenExplicitlyAskedFor()
    {
        assertUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.ONLY_WHEN_HOT, 42 ),
                "CYPHER expressionEngine=COMPILED RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldNotUseCompiledExpressionsWhenExplicitlyAskingForInterpreted()
    {
        assertNotUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.COMPILED, 42 ),
                "CYPHER expressionEngine=INTERPRETED RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsEvenIfPotentiallyCached()
    {
        // Given
        String query = "RETURN sin(cos(sin(cos(rand()))))";
        GraphDatabaseService db = withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.INTERPRETED, 0 );

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( query );
        }

        // Then
        assertUsingCompiled( db, "CYPHER expressionEngine=COMPILED " + query );
    }

    private GraphDatabaseService withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine engine, int limit )
    {

        managementService = new TestEnterpriseDatabaseManagementServiceBuilder()
                .impermanent()
                .setInternalLogProvider( logProvider )
                .setConfig( GraphDatabaseSettings.cypher_runtime, GraphDatabaseSettings.CypherRuntime.SLOTTED )
                .setConfig( GraphDatabaseSettings.cypher_expression_engine, engine )
                .setConfig( GraphDatabaseSettings.cypher_expression_recompilation_limit, limit )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void assertUsingCompiled( GraphDatabaseService db, String query )
    {
        logProvider.clear();
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( query ).resultAsString();
            transaction.commit();
        }

        logProvider.assertAtLeastOnce(
                AssertableLogProvider.inLog( EnterpriseCompilerFactory.class )
                                     .debug( anyOf(
                                containsString( "Compiling expression:" ),
                                containsString( "Compiling projection:" )
                        ) ) );
    }

    private void assertNotUsingCompiled( GraphDatabaseService db, String query )
    {
        logProvider.clear();
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( query ).resultAsString();
            transaction.commit();
        }

        logProvider.assertNone(
                AssertableLogProvider.inLog( EnterpriseCompilerFactory.class )
                                     .debug( anyOf(
                                containsString( "Compiling expression:" ),
                                containsString( "Compiling projection:" )
                        ) ) );
    }

}
