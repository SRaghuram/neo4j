/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import org.neo4j.causalclustering.scenarios.BaseClusterOverviewIT;

class CommercialClusterOverviewIT
{
    @Nested
    @DisplayName( "discovery-akka" )
    class Akka extends BaseClusterOverviewIT
    {
        Akka()
        {
            super( CommercialDiscoveryServiceType.AKKA );
        }
    }
}
