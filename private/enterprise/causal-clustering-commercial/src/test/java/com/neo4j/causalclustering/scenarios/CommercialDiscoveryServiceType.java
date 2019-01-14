/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.AkkaUncleanShutdownDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.akka.CommercialAkkaDiscoveryServiceFactory;

import java.util.function.Supplier;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.scenarios.DiscoveryServiceType;

public enum CommercialDiscoveryServiceType implements DiscoveryServiceType
{
    AKKA( CommercialAkkaDiscoveryServiceFactory::new ),
    AKKA_UNCLEAN_SHUTDOWN( AkkaUncleanShutdownDiscoveryServiceFactory::new ),
    HAZELCAST( SslHazelcastDiscoveryServiceFactory::new );

    private final Supplier<DiscoveryServiceFactory> supplier;

    CommercialDiscoveryServiceType( Supplier<DiscoveryServiceFactory> supplier )
    {
        this.supplier = supplier;
    }

    @Override
    public DiscoveryServiceFactory createFactory()
    {
        return supplier.get();
    }
}
