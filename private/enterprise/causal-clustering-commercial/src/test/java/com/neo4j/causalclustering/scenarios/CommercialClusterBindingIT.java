/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.CommercialDiscoveryServiceType;
import org.junit.runners.Parameterized;

import org.neo4j.causalclustering.scenarios.BaseClusterBindingIT;
import org.neo4j.causalclustering.scenarios.DiscoveryServiceType;

public class CommercialClusterBindingIT extends BaseClusterBindingIT
{

    public CommercialClusterBindingIT( DiscoveryServiceType discoveryServiceType )
    {
        super( discoveryServiceType );
    }

    @Parameterized.Parameters( name = "discovery-{0}" )
    public static DiscoveryServiceType[] data()
    {
        return CommercialDiscoveryServiceType.values();
    }
}
