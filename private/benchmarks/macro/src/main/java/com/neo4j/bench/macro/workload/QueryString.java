/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.tool.macro.ExecutionMode;

import java.util.Arrays;

public abstract class QueryString
{
    private static final String CYPHER_PREFIX = "CYPHER ";

    protected abstract String stableValue();

    protected abstract String rawValue();

    public abstract QueryString copyWith( Runtime newRuntime );

    public abstract QueryString copyWith( Planner newPlanner );

    public abstract QueryString copyWith( ExecutionMode newExecutionMode );

    public abstract boolean isPeriodicCommit();

    private final Planner planner;
    private final Runtime runtime;
    private final ExecutionMode executionMode;

    QueryString( Planner planner, Runtime runtime, ExecutionMode executionMode )
    {
        this.planner = planner;
        this.runtime = runtime;
        this.executionMode = executionMode;
    }

    public String value()
    {
        String baseQueryString = rawValue();
        switch ( executionMode )
        {
        case EXECUTE:
            return withRuntime( withPlanner( baseQueryString, planner ), runtime );
        case PLAN:
            return withRuntime( withPlanner( withExplain( baseQueryString ), planner ), runtime );
        case CARDINALITY:
            return withRuntime( withPlanner( withProfile( baseQueryString ), planner ), runtime );
        default:
            throw new RuntimeException( "Unsupported execution mode: " + executionMode );
        }
    }

    public Planner planner()
    {
        return planner;
    }

    public Runtime runtime()
    {
        return runtime;
    }

    public ExecutionMode executionMode()
    {
        return executionMode;
    }

    private static String withExplain( String query )
    {
        assertExplainNotSpecified( query );

        query = withCypher( query );
        int offset = CYPHER_PREFIX.length();
        return new StringBuilder( query ).insert( offset, "EXPLAIN " ).toString();
    }

    private static String withProfile( String query )
    {
        assertProfileNotSpecified( query );

        query = withCypher( query );
        int offset = CYPHER_PREFIX.length();
        return new StringBuilder( query ).insert( offset, "PROFILE " ).toString();
    }

    private static String withPlanner( String query, Planner planner )
    {
        assertPlannerNotSpecified( query );

        if ( planner.equals( Planner.DEFAULT ) )
        {
            return query;
        }
        else
        {
            query = withCypher( query );
            int offset = CYPHER_PREFIX.length();
            return new StringBuilder( query ).insert( offset, "planner=" + planner.value() + " " ).toString();
        }
    }

    private static String withRuntime( String query, Runtime runtime )
    {
        assertRuntimeNotSpecified( query );

        if ( runtime.equals( Runtime.DEFAULT ) )
        {
            return query;
        }
        else
        {
            query = withCypher( query );
            int offset = CYPHER_PREFIX.length();
            return new StringBuilder( query ).insert( offset, "runtime=" + runtime.value() + " " ).toString();
        }
    }

    private static String withCypher( String query )
    {
        return hasCypher( query ) ? query : CYPHER_PREFIX + query;
    }

    private static void assertExplainNotSpecified( String queryString )
    {
        if ( hasCypher( queryString ) && hasExplain( queryString ) )
        {
            throw new RuntimeException( "'EXPLAIN' already specified in: " + queryString );
        }
    }

    private static void assertProfileNotSpecified( String queryString )
    {
        if ( hasCypher( queryString ) && hasProfile( queryString ) )
        {
            throw new RuntimeException( "'PROFILE' already specified in: " + queryString );
        }
    }

    private static void assertPlannerNotSpecified( String queryString )
    {
        if ( hasCypher( queryString ) && hasPlanner( queryString ) )
        {
            throw new RuntimeException( "Planner already specified in: " + queryString );
        }
    }

    private static void assertRuntimeNotSpecified( String queryString )
    {
        if ( hasCypher( queryString ) && hasRuntime( queryString ) )
        {
            throw new RuntimeException( "Runtime already specified in: " + queryString );
        }
    }

    private static boolean hasCypher( String query )
    {
        return query.toLowerCase().startsWith( CYPHER_PREFIX.toLowerCase() );
    }

    private static boolean hasExplain( String query )
    {
        // TODO regex
        return query.toLowerCase().contains( "explain " );
    }

    private static boolean hasProfile( String query )
    {
        // TODO regex
        return query.toLowerCase().contains( "profile " );
    }

    private static boolean hasPlanner( String query )
    {
        // TODO regex
        String lowerCaseQuery = query.toLowerCase();
        return Arrays.stream( Planner.values() )
                     .anyMatch( planner -> lowerCaseQuery.contains( "planner=" + planner.value() + " " ) );
    }

    private static boolean hasRuntime( String query )
    {
        // TODO regex
        String lowerCaseQuery = query.toLowerCase();
        return Arrays.stream( Runtime.values() )
                     .anyMatch( runtime -> lowerCaseQuery.contains( "runtime=" + runtime.value() + " " ) );
    }
}
