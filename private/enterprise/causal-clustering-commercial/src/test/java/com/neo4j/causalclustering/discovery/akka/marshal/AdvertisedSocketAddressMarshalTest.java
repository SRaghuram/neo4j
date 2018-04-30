/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import org.neo4j.helpers.AdvertisedSocketAddress;

public class AdvertisedSocketAddressMarshalTest extends BaseMarshalTest<AdvertisedSocketAddress>
{
    public AdvertisedSocketAddressMarshalTest()
    {
        super( new AdvertisedSocketAddress( "host", 879 ), new AdvertisedSocketAddressMarshal() );
    }
}
