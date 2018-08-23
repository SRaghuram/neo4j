/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.neo4j.causalclustering.scenarios.BaseMultiClusterRoutingIT;

import static com.neo4j.causalclustering.scenarios.CommercialDiscoveryServiceType.AKKA;

public class CommercialMultiClusterRoutingIT extends BaseMultiClusterRoutingIT
{
    public CommercialMultiClusterRoutingIT( String ignoredName, int numCores, int numReplicas, Set<String> dbNames,
            CommercialDiscoveryServiceType discoveryType )
    {
        super( ignoredName, numCores, numReplicas, dbNames, discoveryType );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]
                {
                        { "[akka discovery, 6 core hosts, 2 databases]", 6, 0, DB_NAMES_1, AKKA },
                }
        );
    }

}
