/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.CommercialDiscoveryServiceType;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.function.Supplier;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.scenarios.BaseClusterOverviewIT;

public class CommercialClusterOverviewIT extends BaseClusterOverviewIT
{
    public CommercialClusterOverviewIT( Supplier<DiscoveryServiceFactory> discoveryServiceType )
    {
        super( discoveryServiceType );
    }

    @Parameterized.Parameters( name = "discovery-{0}" )
    public static Collection<Supplier<DiscoveryServiceFactory>> data()
    {
        return CommercialDiscoveryServiceType.values;
    }
}
