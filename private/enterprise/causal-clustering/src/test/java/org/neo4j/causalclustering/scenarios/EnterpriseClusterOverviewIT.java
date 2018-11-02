/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.runners.Parameterized;

public class EnterpriseClusterOverviewIT extends BaseClusterOverviewIT
{
    public EnterpriseClusterOverviewIT( DiscoveryServiceType discoveryServiceType )
    {
        super( discoveryServiceType );
    }

    @Parameterized.Parameters( name = "discovery-{0}" )
    public static DiscoveryServiceType[] data()
    {
        return EnterpriseDiscoveryServiceType.values();
    }
}
