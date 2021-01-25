/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import org.junit.jupiter.api.Nested;

import static com.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static com.neo4j.causalclustering.discovery.IpFamily.IPV6;

class SecureClusterIpFamilyIT
{
    @Nested
    class AkkaIpv4NoWildcard extends BaseClusterIpFamilyIT
    {
        AkkaIpv4NoWildcard()
        {
            super( DiscoveryServiceType.AKKA, IPV4, false );
        }
    }

    @Nested
    class AkkaIpv6NoWildcard extends BaseClusterIpFamilyIT
    {
        AkkaIpv6NoWildcard()
        {
            super( DiscoveryServiceType.AKKA, IPV6, false );
        }
    }

    @Nested
    class AkkaIpv4Wildcard extends BaseClusterIpFamilyIT
    {
        AkkaIpv4Wildcard()
        {
            super( DiscoveryServiceType.AKKA, IPV4, true );
        }
    }

    @Nested
    class AkkaIpv6Wildcard extends BaseClusterIpFamilyIT
    {
        AkkaIpv6Wildcard()
        {
            super( DiscoveryServiceType.AKKA, IPV6, true );
        }
    }
}
