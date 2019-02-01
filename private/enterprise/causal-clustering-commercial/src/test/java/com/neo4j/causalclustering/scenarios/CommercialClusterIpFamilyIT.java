/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.CommercialDiscoveryServiceType;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.scenarios.BaseClusterIpFamilyIT;

import static org.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static org.neo4j.causalclustering.discovery.IpFamily.IPV6;

public class CommercialClusterIpFamilyIT extends BaseClusterIpFamilyIT
{
    @Parameterized.Parameters( name = "{0} {1} useWildcard={2}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {CommercialDiscoveryServiceType.AKKA, IPV4, false},
                {CommercialDiscoveryServiceType.AKKA, IPV6, false},

                {CommercialDiscoveryServiceType.AKKA, IPV4, true},
                {CommercialDiscoveryServiceType.AKKA, IPV6, true}
        } );
    }

    public CommercialClusterIpFamilyIT( CommercialDiscoveryServiceType discoveryServiceType, IpFamily ipFamily, boolean useWildcard )
    {
        super( discoveryServiceType, ipFamily, useWildcard );
    }
}
