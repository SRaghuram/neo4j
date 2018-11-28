/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.runners.Parameterized;

class EnterpriseClusterOverviewIT
{
    @Nested
    @DisplayName( "hazelcast" )
    class Hz extends BaseClusterOverviewIT
    {
        Hz()
        {
            super( EnterpriseDiscoveryServiceType.HAZELCAST );
        }
    }

    @Nested
    @DisplayName( "shared" )
    class Shared extends BaseClusterOverviewIT
    {
        Shared()
        {
            super( EnterpriseDiscoveryServiceType.SHARED );
        }
    }
}
