/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Collection;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

public abstract class RetryingHostnameResolver implements HostnameResolver
{
    private final int minResolvedAddresses;
    private final MultiRetryStrategy<AdvertisedSocketAddress,Collection<AdvertisedSocketAddress>> retryStrategy;

    RetryingHostnameResolver( Config config, MultiRetryStrategy<AdvertisedSocketAddress,Collection<AdvertisedSocketAddress>> retryStrategy )
    {
        minResolvedAddresses = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
        this.retryStrategy = retryStrategy;
    }

    static MultiRetryStrategy<AdvertisedSocketAddress,Collection<AdvertisedSocketAddress>> defaultRetryStrategy( Config config, LogProvider logProvider )
    {
        long retryIntervalMillis = config.get( CausalClusteringSettings.discovery_resolution_retry_interval ).toMillis();
        long clusterBindingTimeout = config.get( CausalClusteringSettings.discovery_resolution_timeout ).toMillis();
        long numRetries = ( clusterBindingTimeout / retryIntervalMillis ) + 1;
        return new MultiRetryStrategy<>( retryIntervalMillis, numRetries, logProvider, RetryingHostnameResolver::sleep );
    }

    public final Collection<AdvertisedSocketAddress> resolve( AdvertisedSocketAddress advertisedSocketAddress )
    {
        return retryStrategy.apply( advertisedSocketAddress, this::resolveOnce, addrs -> addrs.size() >= minResolvedAddresses );
    }

    protected abstract Collection<AdvertisedSocketAddress> resolveOnce( AdvertisedSocketAddress advertisedSocketAddress );

    private static void sleep( long durationInMillis )
    {
        try
        {
            Thread.sleep( durationInMillis );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
