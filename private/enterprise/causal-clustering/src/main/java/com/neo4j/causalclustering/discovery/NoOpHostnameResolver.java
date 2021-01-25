/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.Collection;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;

import static java.util.Collections.singleton;

public class NoOpHostnameResolver implements HostnameResolver
{
    public static RemoteMembersResolver resolver( Config config )
    {
        return new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );
    }

    @Override
    public Collection<SocketAddress> resolve( SocketAddress advertisedSocketAddresses )
    {
        return singleton(advertisedSocketAddresses);
    }

    @Override
    public boolean useOverrides()
    {
        return true;
    }
}
