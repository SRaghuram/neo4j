/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.utils;

public class AnnotatedQuery
{
    private final String queryString;

    public AnnotatedQuery(
            String queryString,
            PlannerType plannerType,
            RuntimeType runtimeType )
    {
        this.queryString = buildQueryString( queryString, plannerType, runtimeType );
    }

    public String queryString()
    {
        return queryString;
    }

    private static String buildQueryString( String queryString, PlannerType plannerType, RuntimeType runtimeType )
    {
        String cypherTypePrefix = "";
        switch ( plannerType )
        {
        case DEFAULT:
            // do nothing
            break;
        case COST:
            cypherTypePrefix += "planner=cost ";
            break;
        default:
            throw new RuntimeException( "Expected known planner but got " + plannerType );
        }
        switch ( runtimeType )
        {
        case DEFAULT:
            // do nothing
            break;
        case INTERPRETED:
            cypherTypePrefix += "runtime=interpreted ";
            break;
        case COMPILED:
            cypherTypePrefix += "runtime=legacy_compiled ";
            break;
        case SLOTTED:
            cypherTypePrefix += "runtime=slotted ";
            break;
        default:
            throw new RuntimeException( "Expected known runtime but got " + runtimeType );
        }
        return (cypherTypePrefix.isEmpty())
               ? queryString
               : "CYPHER " + cypherTypePrefix + queryString;
    }
}
