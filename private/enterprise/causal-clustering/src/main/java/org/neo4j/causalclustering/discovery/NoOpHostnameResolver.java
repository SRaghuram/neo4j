/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Collection;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.singleton;

public class NoOpHostnameResolver implements HostnameResolver
{
    public static RemoteMembersResolver resolver( Config config )
    {
        return new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );
    }

    @Override
    public Collection<AdvertisedSocketAddress> resolve( AdvertisedSocketAddress advertisedSocketAddresses )
    {
        return singleton(advertisedSocketAddresses);
    }

    @Override
    public boolean useOverrides()
    {
        return true;
    }
}
