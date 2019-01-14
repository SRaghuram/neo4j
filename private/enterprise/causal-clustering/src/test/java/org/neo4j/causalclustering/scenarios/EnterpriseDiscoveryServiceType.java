/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import java.util.function.Supplier;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;

public enum EnterpriseDiscoveryServiceType implements DiscoveryServiceType
{
    SHARED( SharedDiscoveryServiceFactory::new ),
    HAZELCAST( HazelcastDiscoveryServiceFactory::new );

    private final Supplier<DiscoveryServiceFactory> supplier;

    EnterpriseDiscoveryServiceType( Supplier<DiscoveryServiceFactory> supplier )
    {
        this.supplier = supplier;
    }

    @Override
    public DiscoveryServiceFactory createFactory()
    {
        return supplier.get();
    }
}
