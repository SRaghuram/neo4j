/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static com.neo4j.causalclustering.discovery.DiscoveryServiceType.AKKA;
import static com.neo4j.causalclustering.discovery.DiscoveryServiceType.SHARED;

class EnterpriseMultiClusterRoutingIT
{

    @Nested
    @DisplayName( "[shared discovery, 6 core hosts, 2 databases]" )
    class Shared6core2db extends BaseMultiClusterRoutingIT
    {
        Shared6core2db()
        {
            super( 6, 0, DB_NAMES_1, SHARED );
        }
    }

    @Nested
    @DisplayName( "[akka discovery, 6 core hosts, 2 databases]" )
    class Akka6Core2Db extends BaseMultiClusterRoutingIT
    {
        Akka6Core2Db()
        {
            super( 6, 0, DB_NAMES_1, AKKA );
        }
    }

    @Nested
    @DisplayName( "[shared discovery, 5 core hosts, 1 database]" )
    class Shared5Core1Db extends BaseMultiClusterRoutingIT
    {
        Shared5Core1Db()
        {
            super( 5, 0, DB_NAMES_2, SHARED );
        }
    }

    @Nested
    @DisplayName( "[akka discovery, 5 core hosts, 1 database]" )
    class Akka5Core1Db extends BaseMultiClusterRoutingIT
    {
        Akka5Core1Db()
        {
            super( 5, 0, DB_NAMES_2, AKKA );
        }
    }

    @Nested
    @DisplayName( "[akka discovery, 6 core hosts, 3 read replicas, 3 databases]" )
    class Akka9Core3Rr3Db extends BaseMultiClusterRoutingIT
    {
        Akka9Core3Rr3Db()
        {
            super( 9, 3, DB_NAMES_3, AKKA );
        }
    }

    @Nested
    @DisplayName( "[shared discovery, 6 core hosts, 3 read replicas, 3 databases]" )
    class Shared8Core2Rr3Db extends BaseMultiClusterRoutingIT
    {
        Shared8Core2Rr3Db()
        {
            super( 8, 2, DB_NAMES_3, SHARED );
        }
    }

}
