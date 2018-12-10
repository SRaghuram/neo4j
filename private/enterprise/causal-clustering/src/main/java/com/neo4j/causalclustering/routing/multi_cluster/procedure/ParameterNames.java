/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster.procedure;

/**
 * Enumerates the parameter names used for the multi-cluster routing procedures
 */
public enum ParameterNames
{
    /**
     * Type: IN
     *
     * An string specifying the database in the multi-cluster for which to provide routers.
     */
    DATABASE( "database" ),

    /**
     * Type: OUT
     *
     * Defines the time-to-live of the returned information,
     * after which it shall be refreshed.
     *
     * Refer to the specific routing plugin deployed to
     * understand the impact of this setting.
     */
    TTL( "ttl" ),

    /**
     * Type: OUT
     *
     * Contains a multimap of database names to router endpoints.
     */
    ROUTERS( "routers" );

    private final String parameterName;

    ParameterNames( String parameterName )
    {
        this.parameterName = parameterName;
    }

    public String parameterName()
    {
        return parameterName;
    }
}
