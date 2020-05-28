/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import com.neo4j.causalclustering.discovery.RemoteMembersResolver;

import org.neo4j.configuration.Config;
import org.neo4j.logging.internal.LogService;

public final class ResolutionResolverFactory
{
    private ResolutionResolverFactory()
    {
    }

    public static RemoteMembersResolver chooseResolver( Config config, LogService logService )
    {
        DiscoveryType discoveryType = config.get( CausalClusteringSettings.discovery_type );
        return discoveryType.getHostnameResolver( logService, config );
    }
}
