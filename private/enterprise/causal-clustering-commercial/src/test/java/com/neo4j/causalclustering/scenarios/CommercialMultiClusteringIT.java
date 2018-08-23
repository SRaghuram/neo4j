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

import org.neo4j.causalclustering.scenarios.BaseMultiClusteringIT;

import static com.neo4j.causalclustering.scenarios.CommercialDiscoveryServiceType.AKKA;

public class CommercialMultiClusteringIT extends BaseMultiClusteringIT
{
    public CommercialMultiClusteringIT( String ignoredName, int numCores, int numReplicas, Set<String> dbNames,
            CommercialDiscoveryServiceType discoveryServiceType )
    {
        super( ignoredName, numCores, numReplicas, dbNames, discoveryServiceType );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]
            {
                { "[akka discovery, 6 core hosts, 2 databases]", 6, 0, DB_NAMES_2, AKKA }
            }
        );
    }

}
