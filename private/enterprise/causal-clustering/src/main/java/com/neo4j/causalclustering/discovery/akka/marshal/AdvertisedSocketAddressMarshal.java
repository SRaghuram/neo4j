/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;

import java.io.IOException;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public class AdvertisedSocketAddressMarshal extends SafeChannelMarshal<AdvertisedSocketAddress>
{
    @Override
    protected AdvertisedSocketAddress unmarshal0( ReadableChannel channel ) throws IOException
    {
        String host = StringMarshal.unmarshal( channel );
        int port = channel.getInt();
        return new AdvertisedSocketAddress( host, port );
    }

    @Override
    public void marshal( AdvertisedSocketAddress advertisedSocketAddress, WritableChannel channel ) throws IOException
    {
        StringMarshal.marshal( channel, advertisedSocketAddress.getHostname() );
        channel.putInt( advertisedSocketAddress.getPort() );
    }
}
