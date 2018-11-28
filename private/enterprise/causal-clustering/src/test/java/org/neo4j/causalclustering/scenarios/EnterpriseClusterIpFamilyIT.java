/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.Nested;

import static org.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static org.neo4j.causalclustering.discovery.IpFamily.IPV6;
import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.HAZELCAST;
import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.SHARED;

class EnterpriseClusterIpFamilyIT
{
    @Nested
    class SharedIpv4NoWildcard extends BaseClusterIpFamilyIT
    {
        SharedIpv4NoWildcard()
        {
            super( SHARED, IPV4, false );
        }
    }

    @Nested
    class SharedIpv6Wildcard extends BaseClusterIpFamilyIT
    {
        SharedIpv6Wildcard()
        {
            super( SHARED, IPV6, true );
        }
    }

    @Nested
    class HazelcastIpv4NoWildcard extends BaseClusterIpFamilyIT
    {
        HazelcastIpv4NoWildcard()
        {
            super( HAZELCAST, IPV4, false );
        }
    }

    @Nested
    class HazelcastIpv6NoWildcard extends BaseClusterIpFamilyIT
    {
        HazelcastIpv6NoWildcard()
        {
            super( HAZELCAST, IPV6, false );
        }
    }

    @Nested
    class HazelcastIpv4Wildcard extends BaseClusterIpFamilyIT
    {
        HazelcastIpv4Wildcard()
        {
            super( HAZELCAST, IPV4, true );
        }
    }

    @Nested
    class HazelcastIpv6Wildcard extends BaseClusterIpFamilyIT
    {
        HazelcastIpv6Wildcard()
        {
            super( HAZELCAST, IPV6, true );
        }
    }
}
