/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import org.junit.jupiter.api.Test;

import com.neo4j.causalclustering.protocol.Protocol;

class CommercialCatchupV1Test extends CommercialCatchupTest
{
    CommercialCatchupV1Test()
    {
        super( Protocol.ApplicationProtocols.CATCHUP_1 );
    }

    @Test
    void shouldHandleStoreIdRequest() throws Exception
    {
        executeTestScenario( storeId() );
    }
}
