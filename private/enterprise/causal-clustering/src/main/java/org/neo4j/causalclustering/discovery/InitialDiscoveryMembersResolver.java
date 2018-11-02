/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;

public class InitialDiscoveryMembersResolver implements RemoteMembersResolver
{
    private final HostnameResolver hostnameResolver;
    private final List<AdvertisedSocketAddress> advertisedSocketAddresses;

    public InitialDiscoveryMembersResolver( HostnameResolver hostnameResolver, Config config )
    {
        this.hostnameResolver = hostnameResolver;
        advertisedSocketAddresses = config.get( CausalClusteringSettings.initial_discovery_members );
    }

    @Override
    public <T> Collection<T> resolve( Function<AdvertisedSocketAddress,T> transform )
    {
        return advertisedSocketAddresses
                .stream()
                .flatMap( raw -> hostnameResolver.resolve( raw ).stream() )
                .map( transform )
                .collect( Collectors.toSet() );
    }
}
