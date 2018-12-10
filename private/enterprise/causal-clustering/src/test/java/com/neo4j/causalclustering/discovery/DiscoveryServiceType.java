/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;

import java.util.function.Supplier;

public enum DiscoveryServiceType
{
    AKKA( AkkaDiscoveryServiceFactory::new ),
    AKKA_UNCLEAN_SHUTDOWN( AkkaUncleanShutdownDiscoveryServiceFactory::new ),
    HAZELCAST_SSL( SslHazelcastDiscoveryServiceFactory::new ),
    SHARED( SharedDiscoveryServiceFactory::new ),
    HAZELCAST( HazelcastDiscoveryServiceFactory::new );

    private final Supplier<DiscoveryServiceFactory> supplier;

    DiscoveryServiceType( Supplier<DiscoveryServiceFactory> supplier )
    {
        this.supplier = supplier;
    }

    public DiscoveryServiceFactory createFactory()
    {
        return supplier.get();
    }
}
