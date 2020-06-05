/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

import java.util.Collection;

import org.neo4j.configuration.helpers.SocketAddress;

import static java.util.Collections.singletonList;

public class AdvertisedSocketAddressMarshalTest extends BaseMarshalTest<SocketAddress>
{
    @Override
    Collection<SocketAddress> originals()
    {
        return singletonList( new SocketAddress( "host", 879 ) );
    }

    @Override
    ChannelMarshal<SocketAddress> marshal()
    {
        return new AdvertisedSocketAddressMarshal();
    }
}
