/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.discovery.IpFamily;

import static org.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static org.neo4j.causalclustering.discovery.IpFamily.IPV6;
import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.HAZELCAST;
import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.SHARED;

public class EnterpriseClusterIpFamilyIT extends BaseClusterIpFamilyIT
{
    @Parameterized.Parameters( name = "{0} {1} useWildcard={2}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {SHARED, IPV4, false},
                {SHARED, IPV6, true},

                {HAZELCAST, IPV4, false},
                {HAZELCAST, IPV6, false},

                {HAZELCAST, IPV4, true},
                {HAZELCAST, IPV6, true},
        } );
    }

    public EnterpriseClusterIpFamilyIT( DiscoveryServiceType discoveryServiceFactory, IpFamily ipFamily, boolean useWildcard )
    {
        super( discoveryServiceFactory, ipFamily, useWildcard );
    }
}
