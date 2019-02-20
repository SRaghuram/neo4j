/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.utils;

public class AnnotatedQuery
{
    public static String withExplain( String cypher )
    {
        return "explain " + cypher;
    }

    public static String withProfile( String cypher )
    {
        return "profile " + cypher;
    }

    private final int operationType;
    private final String operationDescription;
    private final String defaultQueryString;
    private final String queryString;
    private final PlannerType plannerType;
    private final RuntimeType runtimeType;

    public AnnotatedQuery(
            int operationType,
            String operationDescription,
            String queryString,
            PlannerType plannerType,
            RuntimeType runtimeType )
    {
        this.operationType = operationType;
        this.operationDescription = operationDescription;
        this.defaultQueryString = queryString;
        this.queryString = buildQueryString( queryString, plannerType, runtimeType );
        this.plannerType = plannerType;
        this.runtimeType = runtimeType;
    }

    public int operationType()
    {
        return operationType;
    }

    public String operationDescription()
    {
        return operationDescription;
    }

    public String defaultQueryString()
    {
        return defaultQueryString;
    }

    public String queryString()
    {
        return queryString;
    }

    public PlannerType plannerType()
    {
        return plannerType;
    }

    public RuntimeType runtimeType()
    {
        return runtimeType;
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
            cypherTypePrefix += "runtime=compiled ";
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
