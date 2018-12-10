/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

import com.neo4j.causalclustering.core.CausalClusteringSettings;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.in_flight_cache_max_bytes;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.in_flight_cache_max_entries;

public class InFlightCacheFactory
{
    public static InFlightCache create( Config config, Monitors monitors )
    {
        return config.get( CausalClusteringSettings.in_flight_cache_type ).create( config, monitors );
    }

    public enum Type
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
}
