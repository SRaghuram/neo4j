/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.HAZELCAST;
import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.SHARED;

public class EnterpriseMultiClusteringIT extends BaseMultiClusteringIT
{
    public EnterpriseMultiClusteringIT( String ignoredName, int numCores, int numReplicas, Set<String> dbNames,
            DiscoveryServiceType discoveryServiceType )
    {
        super( ignoredName, numCores, numReplicas, dbNames, discoveryServiceType );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]
                {
                        { "[shared discovery, 6 core hosts, 2 databases]", 6, 0, DB_NAMES_2, SHARED },
                        { "[hazelcast discovery, 6 core hosts, 2 databases]", 6, 0, DB_NAMES_2, HAZELCAST },
                        { "[shared discovery, 5 core hosts, 1 database]", 5, 0, DB_NAMES_1, SHARED },
                        { "[hazelcast discovery, 5 core hosts, 2 databases]", 5, 0, DB_NAMES_2, HAZELCAST },
                        { "[hazelcast discovery, 9 core hosts, 3 read replicas, 3 databases]", 9, 3, DB_NAMES_3, HAZELCAST },
                        { "[shared discovery, 8 core hosts, 2 read replicas, 3 databases]", 8, 2, DB_NAMES_3, SHARED }
                }
        );
    }
}
