/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;

import org.neo4j.configuration.Config;
import org.neo4j.monitoring.Monitors;

public final class InFlightCacheFactory
{
    private InFlightCacheFactory()
    {
    }

    public static InFlightCache create( Config config, Monitors monitors )
    {
        return config.get( CausalClusteringSettings.in_flight_cache_type ).create( config, monitors );
    }
}
