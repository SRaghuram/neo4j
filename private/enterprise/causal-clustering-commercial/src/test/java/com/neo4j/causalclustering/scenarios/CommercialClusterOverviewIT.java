/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import org.junit.runners.Parameterized;

import java.util.Collections;

import org.neo4j.causalclustering.scenarios.BaseClusterOverviewIT;

public class CommercialClusterOverviewIT extends BaseClusterOverviewIT
{
    public CommercialClusterOverviewIT( CommercialDiscoveryServiceType discoveryServiceType )
    {
        super( discoveryServiceType );
    }

    @Parameterized.Parameters( name = "discovery-{0}" )
    public static Iterable<CommercialDiscoveryServiceType> data()
    {
        return Collections.singletonList( CommercialDiscoveryServiceType.AKKA );
    }
}
