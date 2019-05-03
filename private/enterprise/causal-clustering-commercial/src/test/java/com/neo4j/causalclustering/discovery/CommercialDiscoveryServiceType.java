/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.CommercialAkkaDiscoveryServiceFactory;

import java.util.function.Supplier;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.scenarios.DiscoveryServiceType;

public enum CommercialDiscoveryServiceType implements DiscoveryServiceType
{
    AKKA( CommercialAkkaDiscoveryServiceFactory::new ),
    HAZELCAST( SslHazelcastDiscoveryServiceFactory::new ),
    SHARED( SslSharedDiscoveryServiceFactory::new );

    private final Supplier<SslDiscoveryServiceFactory> supplier;

    CommercialDiscoveryServiceType( Supplier<SslDiscoveryServiceFactory> supplier )
    {
        this.supplier = supplier;
    }

    @Override
    public SslDiscoveryServiceFactory createFactory()
    {
        return supplier.get();
    }

    /**
     * A version of Akka discovery service that does not cleanly shutdown cluster members. They just stop, and must be removed by the rest of the cluster.
     * Intended to simulate a member crash or network partition, and only for use in tests that test recovery of clusters from such situations.
     */
    public static DiscoveryServiceType AKKA_UNCLEAN_SHUTDOWN = new DiscoveryServiceType()
    {
        @Override
        public DiscoveryServiceFactory createFactory()
        {
            return new AkkaUncleanShutdownDiscoveryServiceFactory();
        }

        @Override
        public String name()
        {
            return "AKKA_UNCLEAN_SHUTDOWN";
        }
    };
}
