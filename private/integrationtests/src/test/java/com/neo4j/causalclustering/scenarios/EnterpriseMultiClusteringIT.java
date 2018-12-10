/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static com.neo4j.causalclustering.discovery.DiscoveryServiceType.HAZELCAST;
import static com.neo4j.causalclustering.discovery.DiscoveryServiceType.SHARED;

class EnterpriseMultiClusteringIT
{

    @Nested
    @DisplayName( "[shared discovery, 6 core hosts, 2 databases]" )
    class Shared6core2db extends BaseMultiClusteringIT
    {

        Shared6core2db()
        {
            super( 6, 0, DB_NAMES_2, SHARED );
        }
    }

    @Nested
    @DisplayName( "[hazelcast discovery, 6 core hosts, 2 databases]" )
    class Hz7core2db extends BaseMultiClusteringIT
    {

        Hz7core2db()
        {
            super( 6, 0, DB_NAMES_2, HAZELCAST );
        }
    }

    @Nested
    @DisplayName( "[shared discovery, 5 core hosts, 1 database]" )
    class Shared5core1db extends BaseMultiClusteringIT
    {

        Shared5core1db()
        {
            super( 5, 0, DB_NAMES_1, SHARED );
        }
    }

    @Nested
    @DisplayName( "[hazelcast discovery, 5 core hosts, 2 databases]" )
    class Hz5core2db extends BaseMultiClusteringIT
    {

        Hz5core2db()
        {
            super( 5, 0, DB_NAMES_2, HAZELCAST );
        }
    }

    @Nested
    @DisplayName( "[hazelcast discovery, 9 core hosts, 3 read replicas, 3 databases]" )
    class Hz9core3rr3db extends BaseMultiClusteringIT
    {

        Hz9core3rr3db()
        {
            super( 9, 3, DB_NAMES_3, HAZELCAST );
        }
    }

    @Nested
    @DisplayName( "[shared discovery, 8 core hosts, 2 read replicas, 3 databases]" )
    class Shared8cre2rr3sb extends BaseMultiClusteringIT
    {

        Shared8cre2rr3sb()
        {
            super( 8, 2, DB_NAMES_3, SHARED );
        }
    }
}
