/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing;

/**
 * Defines a capability of an endpoint.
 *
 * Note that a particular server might have several capabilities
 * but they will all be reported as distinct endpoints.
 */
public enum Role
{
    /**
     * Supports read operations.
     */
    READ,

    /**
     * Supports write operations.
     */
    WRITE,

    /**
     * Supports the GetServers procedure and is thus
     * capable of participating in discovery and load
     * balancing.
     */
    ROUTE
}
