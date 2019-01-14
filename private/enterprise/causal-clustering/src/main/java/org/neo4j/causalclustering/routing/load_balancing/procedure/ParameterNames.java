/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.procedure;

/**
 * Enumerates the parameter names used for the GetServers
 * procedures in a causal cluster.
 */
public enum ParameterNames
{
    /**
     * Type: IN
     *
     * An opaque key-value map for supplying client context.
     *
     * Refer to the specific routing plugin deployed to
     * understand which specific keys can be utilised.
     */
    CONTEXT( "context" ),

    /**
     * Type: OUT
     *
     * Contains a map of endpoints and their associated capability.
     *
     * Refer to the protocol specification to understand the
     * exact format and how to utilise it.
     */
    SERVERS( "servers" ),

    /**
     * Type: OUT
     *
     * Defines the time-to-live of the returned information,
     * after which it shall be refreshed.
     *
     * Refer to the specific routing plugin deployed to
     * understand the impact of this setting.
     */
    TTL( "ttl" );

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
