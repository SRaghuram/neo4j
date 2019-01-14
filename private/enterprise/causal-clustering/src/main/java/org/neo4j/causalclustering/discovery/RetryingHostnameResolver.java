/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Collection;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;

public abstract class RetryingHostnameResolver implements HostnameResolver
{
    private final int minResolvedAddresses;
    private final RetryStrategy retryStrategy;

    RetryingHostnameResolver( Config config, RetryStrategy retryStrategy )
    {
        minResolvedAddresses = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation );
        this.retryStrategy = retryStrategy;
    }

    static RetryStrategy defaultRetryStrategy( Config config )
    {
        long retryIntervalMillis = config.get( CausalClusteringSettings.discovery_resolution_retry_interval ).toMillis();
        long clusterBindingTimeout = config.get( CausalClusteringSettings.discovery_resolution_timeout ).toMillis();
        long numRetries = ( clusterBindingTimeout / retryIntervalMillis ) + 1;
        return new RetryStrategy( retryIntervalMillis, numRetries );
    }

    public final Collection<AdvertisedSocketAddress> resolve( AdvertisedSocketAddress advertisedSocketAddress )
    {
        try
        {
            return retryStrategy.apply( () -> resolveOnce( advertisedSocketAddress ), addrs -> addrs.size() >= minResolvedAddresses );
        }
        catch ( TimeoutException e )
        {
            // another instance may still have resolved enough members to bootstrap. Let ClusterBinder decide
            return resolveOnce( advertisedSocketAddress );
        }
    }

    protected abstract Collection<AdvertisedSocketAddress> resolveOnce( AdvertisedSocketAddress advertisedSocketAddress );
}
