/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;

public enum DiscoveryServiceType
{
    AKKA( new AkkaDiscoveryServiceFactory() ),

    /**
     * A version of Akka discovery service that does not cleanly shutdown cluster members. They just stop, and must be removed by the rest of the cluster.
     * Intended to simulate a member crash or network partition, and only for use in tests that test recovery of clusters from such situations.
     */
    AKKA_UNCLEAN_SHUTDOWN( new AkkaUncleanShutdownDiscoveryServiceFactory() );

    private final DiscoveryServiceFactory discoveryServiceFactory;

    DiscoveryServiceType( DiscoveryServiceFactory discoveryServiceFactory )
    {
        this.discoveryServiceFactory = discoveryServiceFactory;
    }

    public DiscoveryServiceFactory factory()
    {
        return discoveryServiceFactory;
    }
}
