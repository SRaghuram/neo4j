/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.LogAssertions.assertThat;

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

    @Test
    void shouldUseCompiledExpressionsWithReplanForceAndDefaultSettings()
    {
        assertUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.DEFAULT, 42 ),
                             "CYPHER replan=force RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldNotUseCompiledExpressionsWithReplanForceAndInterpretedSettings()
    {
        assertNotUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.INTERPRETED, 42 ),
                                "CYPHER replan=force RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldNotUseCompiledExpressionsWithReplanForceWhenExplicitlyAskingForInterpreted()
    {
        assertNotUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.COMPILED, 42 ),
                                "CYPHER expressionEngine=INTERPRETED replan=force RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldNotUseCompiledExpressionsWithReplanSkipAndDefaultSettings()
    {
        assertNotUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.DEFAULT, 42 ),
                                "CYPHER replan=skip RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsWithReplanSkipAndCompiledSettings()
    {
        assertUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.COMPILED, 42 ),
                             "CYPHER replan=skip RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsWithReplanSkipWhenExplicitlyAskingForCompiled()
    {
        assertUsingCompiled( withEngineAndLimit( GraphDatabaseSettings.CypherExpressionEngine.INTERPRETED, 42 ),
                             "CYPHER expressionEngine=COMPILED replan=skip RETURN sin(cos(sin(cos(rand()))))" );
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

        assertThat( logProvider ).forClass( EnterpriseCompilerFactory.class ).forLevel( DEBUG )
                .satisfiesAnyOf(
                        logProvider -> assertThat( logProvider ).containsMessages( "Compiling expression:" ),
                        logProvider -> assertThat( logProvider ).containsMessages( "Compiling projection:" ) );
    }

    private void assertNotUsingCompiled( GraphDatabaseService db, String query )
    {
        logProvider.clear();
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( query ).resultAsString();
            transaction.commit();
        }

        assertThat( logProvider ).forClass( EnterpriseCompilerFactory.class ).forLevel( DEBUG )
                .doesNotContainMessage( "Compiling expression:" )
                .doesNotContainMessage( "Compiling projection:" );
    }

}
