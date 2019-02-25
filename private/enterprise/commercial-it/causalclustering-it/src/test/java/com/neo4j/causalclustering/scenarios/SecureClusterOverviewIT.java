/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

class SecureClusterOverviewIT
{
    @Nested
    @DisplayName( "discovery-akka" )
    class Akka extends BaseClusterOverviewIT
    {
        Akka()
        {
            super( DiscoveryServiceType.AKKA );
        }
    }
}
