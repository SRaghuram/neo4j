/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.akka.CommercialAkkaDiscoveryServiceFactory;

import org.neo4j.causalclustering.scenarios.CoreTopologyServiceIT;

public class AkkaCoreTopologyServiceIT extends CoreTopologyServiceIT
{
    public AkkaCoreTopologyServiceIT()
    {
        super( CommercialAkkaDiscoveryServiceFactory::new );
    }
}
