/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.CausalClusteringSettings;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;

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
    public <C extends Collection<T>,T> C resolve( Function<AdvertisedSocketAddress,T> transform, Supplier<C> collectionFactory )
    {
        return advertisedSocketAddresses
                .stream()
                .flatMap( raw -> hostnameResolver.resolve( raw ).stream() )
                .sorted( advertisedSockedAddressComparator )
                .distinct()
                .map( transform )
                .collect( Collectors.toCollection( collectionFactory ) );
    }

    public static final Comparator<AdvertisedSocketAddress> advertisedSockedAddressComparator =
            Comparator.comparing( AdvertisedSocketAddress::getHostname ).thenComparingInt( AdvertisedSocketAddress::getPort );

    public static Comparator<AdvertisedSocketAddress> advertisedSocketAddressComparator()
    {
        return advertisedSockedAddressComparator;
    }

    @Override
    public boolean useOverrides()
    {
        return hostnameResolver.useOverrides();
    }
}
