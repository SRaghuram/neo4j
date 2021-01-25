/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.cluster.UniqueAddress;

public class UniqueAddressSerializer extends BaseAkkaSerializer<UniqueAddress>
{
    private static final int SIZE_HINT = 256;

    public UniqueAddressSerializer()
    {
        super( new UniqueAddressMarshal(), UNIQUE_ADDRESS, SIZE_HINT );
    }
}
