/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;

import java.util.function.Supplier;

public interface DiscoveryServiceType
{
    DiscoveryServiceFactory createFactory();

    enum Reliable implements DiscoveryServiceType
    {
        AKKA( AkkaDiscoveryServiceFactory::new ),
        SHARED( SharedDiscoveryServiceFactory::new );

        private final Supplier<DiscoveryServiceFactory> supplier;

        Reliable( Supplier<DiscoveryServiceFactory> supplier )
        {
            this.supplier = supplier;
        }

        @Override
        public DiscoveryServiceFactory createFactory()
        {
            return supplier.get();
        }
    }

    interface Unreliable
    {
        /**
         * A version of Akka discovery service that does not cleanly shutdown cluster members. They just stop, and must be removed by the rest of the cluster.
         * Intended to simulate a member crash or network partition, and only for use in tests that test recovery of clusters from such situations.
         */
        DiscoveryServiceType AKKA_UNCLEAN_SHUTDOWN = AkkaUncleanShutdownDiscoveryServiceFactory::new;
    }
}

