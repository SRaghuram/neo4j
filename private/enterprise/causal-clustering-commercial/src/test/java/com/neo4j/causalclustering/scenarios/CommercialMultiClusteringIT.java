/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import org.neo4j.causalclustering.scenarios.BaseMultiClusteringIT;

import static com.neo4j.causalclustering.scenarios.CommercialDiscoveryServiceType.AKKA;

class CommercialMultiClusteringIT
{
    @Nested
    @DisplayName( "[akka discovery, 6 core hosts, 2 databases]" )
    class Akka6core2db extends BaseMultiClusteringIT
    {
        Akka6core2db()
        {
            super( 6, 0, DB_NAMES_2, AKKA );
        }
    }
}
