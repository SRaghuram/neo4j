/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import org.neo4j.causalclustering.scenarios.BaseMultiClusterRoutingIT;

import static com.neo4j.causalclustering.scenarios.CommercialDiscoveryServiceType.AKKA;

class CommercialMultiClusterRoutingIT
{
    @Nested
    @DisplayName( "[akka discovery, 6 core hosts, 2 databases]" )
    class Akka6cores1Db extends BaseMultiClusterRoutingIT
    {
        Akka6cores1Db()
        {
            super( 6, 0, DB_NAMES_1, AKKA );
        }
    }
}
