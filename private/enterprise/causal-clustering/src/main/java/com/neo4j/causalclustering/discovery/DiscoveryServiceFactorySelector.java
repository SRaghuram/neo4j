/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;

import org.neo4j.kernel.configuration.Config;

public final class DiscoveryServiceFactorySelector
{
    public static final DiscoveryImplementation DEFAULT = DiscoveryImplementation.HAZELCAST;

    public DiscoveryServiceFactory select( Config config )
    {
        DiscoveryImplementation middleware = config.get( CausalClusteringSettings.discovery_implementation );

        boolean sslEnabled = config.get( CausalClusteringSettings.ssl_policy ) != null ;
        return select( middleware, sslEnabled );
    }

    private DiscoveryServiceFactory select( DiscoveryImplementation middleware, boolean sslEnabled )
    {
        switch ( middleware )
        {
        case HAZELCAST:
            return sslEnabled ? new SslHazelcastDiscoveryServiceFactory() : new HazelcastDiscoveryServiceFactory();
        case AKKA:
            return new AkkaDiscoveryServiceFactory();
        default:
            throw new IllegalArgumentException( "Should have matched a discovery service factory to " + middleware );
        }
    }
}
