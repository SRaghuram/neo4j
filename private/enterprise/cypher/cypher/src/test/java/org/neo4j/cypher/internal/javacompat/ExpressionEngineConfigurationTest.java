/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.javacompat;

import org.junit.jupiter.api.Test;

import org.neo4j.cypher.internal.EnterpriseCompilerFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.anyOf;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class ExpressionEngineConfigurationTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void shouldBeJitCompileOnSecondAccessByDefault()
    {
        // Given
        String query = "RETURN sin(cos(sin(cos(rand()))))";
        GraphDatabaseService db = withEngineAndLimit( "DEFAULT", 1 );

        assertNotUsingCompiled( db, query );

        assertUsingCompiled( db, query );
    }

    @Test
    void shouldNotUseCompiledExpressionsFirstTimeWithJitEnabled()
    {
        assertNotUsingCompiled( withEngineAndLimit( "ONLY_WHEN_HOT", 1 ), "RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsFirstTimeWhenLimitIsZero()
    {
        assertUsingCompiled( withEngineAndLimit( "ONLY_WHEN_HOT", 0 ), "RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsWhenQueryIsHotWithJitEnabled()
    {
        // Given
        String query = "RETURN sin(cos(sin(cos(rand()))))";
        GraphDatabaseService db = withEngineAndLimit( "ONLY_WHEN_HOT", 3 );

        // When
        db.execute( query );
        db.execute( query );
        db.execute( query );

        // Then
        assertUsingCompiled( db, query );
    }

    @Test
    void shouldUseCompiledExpressionsFirstTimeWhenConfigured()
    {
        assertUsingCompiled( withEngineAndLimit( "COMPILED", 42 ), "RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsFirstTimeWhenExplicitlyAskedFor()
    {
        assertUsingCompiled( withEngineAndLimit( "ONLY_WHEN_HOT", 42 ),
                "CYPHER expressionEngine=COMPILED RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldNotUseCompiledExpressionsWhenExplicitlyAskingForInterpreted()
    {
        assertNotUsingCompiled( withEngineAndLimit( "COMPILED", 42 ),
                "CYPHER expressionEngine=INTERPRETED RETURN sin(cos(sin(cos(rand()))))" );
    }

    @Test
    void shouldUseCompiledExpressionsEvenIfPotentiallyCached()
    {
        // Given
        String query = "RETURN sin(cos(sin(cos(rand()))))";
        GraphDatabaseService db = withEngineAndLimit( "INTERPRETED", 0 );

        // When
        db.execute( query );

        // Then
        assertUsingCompiled( db, "CYPHER expressionEngine=COMPILED " + query );
    }

    private GraphDatabaseService withEngineAndLimit( String engine, int limit )
    {

        return new TestGraphDatabaseFactory().
                setInternalLogProvider( logProvider )
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.cypher_expression_engine, engine )
                .setConfig( GraphDatabaseSettings.cypher_expression_recompilation_limit, Integer.toString( limit ) )
                .newGraphDatabase();
    }

    private void assertUsingCompiled( GraphDatabaseService db, String query )
    {
        logProvider.clear();
        db.execute( query ).resultAsString();

        logProvider.assertAtLeastOnce(
                inLog( EnterpriseCompilerFactory.class )
                        .debug( anyOf(
                                containsString( "Compiling expression:" ),
                                containsString( "Compiling projection:" )
                        ) ) );
    }

    private void assertNotUsingCompiled( GraphDatabaseService db, String query )
    {
        logProvider.clear();
        db.execute( query ).resultAsString();

        logProvider.assertNone(
                inLog( EnterpriseCompilerFactory.class )
                        .debug( anyOf(
                                containsString( "Compiling expression:" ),
                                containsString( "Compiling projection:" )
                        ) ) );
    }

}
