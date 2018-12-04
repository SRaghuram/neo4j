/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.akka.AkkaCoreTopologyService;
import org.junit.jupiter.api.Test;

import org.neo4j.causalclustering.scenarios.BaseCoreTopologyServiceIT;

class AkkaCoreTopologyServiceIT extends BaseCoreTopologyServiceIT
{
    AkkaCoreTopologyServiceIT()
    {
        super( CommercialDiscoveryServiceType.AKKA );
    }

    @Test
    void shouldRestart() throws Throwable
    {
        service.init();
        service.start();
        ((AkkaCoreTopologyService)service).restart();
        service.stop();
        service.shutdown();
    }
}
