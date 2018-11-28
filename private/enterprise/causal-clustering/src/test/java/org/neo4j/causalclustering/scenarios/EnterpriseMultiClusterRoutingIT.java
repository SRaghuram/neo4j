/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.HAZELCAST;
import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.SHARED;

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
    @DisplayName( "[hazelcast discovery, 6 core hosts, 2 databases]" )
    class Hz6core2db extends BaseMultiClusterRoutingIT
    {
        Hz6core2db()
        {
            super( 6, 0, DB_NAMES_1, HAZELCAST );
        }
    }

    @Nested
    @DisplayName( "[shared discovery, 5 core hosts, 1 database]" )
    class Shared5core1db extends BaseMultiClusterRoutingIT
    {
        Shared5core1db()
        {
            super( 5, 0, DB_NAMES_2, SHARED );
        }
    }

    @Nested
    @DisplayName( "[hazelcast discovery, 5 core hosts, 1 database]" )
    class Hz5core1db extends BaseMultiClusterRoutingIT
    {
        Hz5core1db()
        {
            super( 5, 0, DB_NAMES_2, HAZELCAST );
        }
    }

    @Nested
    @DisplayName( "[hazelcast discovery, 6 core hosts, 3 read replicas, 3 databases]" )
    class Hz9core3rr3db extends BaseMultiClusterRoutingIT
    {
        Hz9core3rr3db()
        {
            super( 9, 3, DB_NAMES_3, HAZELCAST );
        }
    }

    @Nested
    @DisplayName( "[shared discovery, 6 core hosts, 3 read replicas, 3 databases]" )
    class Shared8core2rr3db extends BaseMultiClusterRoutingIT
    {
        Shared8core2rr3db()
        {
            super( 8, 2, DB_NAMES_3, SHARED );
        }
    }

}
