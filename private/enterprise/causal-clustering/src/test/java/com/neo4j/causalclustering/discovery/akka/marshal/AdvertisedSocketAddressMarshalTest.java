/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import java.util.Collection;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.marshal.ChannelMarshal;

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
