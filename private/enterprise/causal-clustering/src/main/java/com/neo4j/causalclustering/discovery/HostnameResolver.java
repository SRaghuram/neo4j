/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.Collection;

import org.neo4j.configuration.helpers.SocketAddress;

public interface HostnameResolver
{
    Collection<SocketAddress> resolve( SocketAddress advertisedSocketAddresses );

    default boolean useOverrides()
    {
        return false;
    }
}
