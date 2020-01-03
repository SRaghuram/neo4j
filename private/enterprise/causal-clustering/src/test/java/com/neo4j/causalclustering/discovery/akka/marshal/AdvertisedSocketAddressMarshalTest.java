/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import org.neo4j.configuration.helpers.SocketAddress;

public class AdvertisedSocketAddressMarshalTest extends BaseMarshalTest<SocketAddress>
{
    public AdvertisedSocketAddressMarshalTest()
    {
        super( new SocketAddress( "host", 879 ), new AdvertisedSocketAddressMarshal() );
    }
}
