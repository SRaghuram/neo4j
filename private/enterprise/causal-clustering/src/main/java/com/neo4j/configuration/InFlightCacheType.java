/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheMonitor;
import com.neo4j.causalclustering.core.consensus.log.cache.UnboundedInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.VoidInFlightCache;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.configuration.Config;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.configuration.CausalClusteringSettings.in_flight_cache_max_bytes;
import static com.neo4j.configuration.CausalClusteringSettings.in_flight_cache_max_entries;

@PublicApi
public enum InFlightCacheType
{
    NONE
            {
                @Override
                InFlightCache create( Config config, Monitors monitors )
                {
                    return new VoidInFlightCache();
                }
            },
    CONSECUTIVE
            {
                @Override
                InFlightCache create( Config config, Monitors monitors )
                {
                    return new ConsecutiveInFlightCache( config.get( in_flight_cache_max_entries ), config.get( in_flight_cache_max_bytes ),
                            monitors.newMonitor( InFlightCacheMonitor.class ), false );
                }
            },
    UNBOUNDED
            {
                @Override
                InFlightCache create( Config config, Monitors monitors )
                {
                    return new UnboundedInFlightCache();
                }
            };

    abstract InFlightCache create( Config config, Monitors monitors );
}
