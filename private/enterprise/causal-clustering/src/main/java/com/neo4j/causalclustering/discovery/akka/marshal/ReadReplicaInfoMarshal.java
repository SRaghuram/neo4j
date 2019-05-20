/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

import java.io.IOException;
import java.util.Set;

import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaInfoMarshal extends DiscoveryServerInfoMarshal<ReadReplicaInfo>
{
    private final ChannelMarshal<ClientConnectorAddresses> clientConnectorAddressesMarshal = new ClientConnectorAddresses.Marshal();
    private final ChannelMarshal<AdvertisedSocketAddress> advertisedSocketAddressMarshal = new AdvertisedSocketAddressMarshal();

    @Override
    protected ReadReplicaInfo unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        ClientConnectorAddresses clientConnectorAddresses = clientConnectorAddressesMarshal.unmarshal( channel );
        AdvertisedSocketAddress catchupServer = advertisedSocketAddressMarshal.unmarshal( channel );
        Set<String> groups = unmarshalGroups( channel );
        Set<DatabaseId> databaseIds = unmarshalDatabaseIds( channel );
        return new ReadReplicaInfo( clientConnectorAddresses, catchupServer, groups, databaseIds );
    }

    @Override
    public void marshal( ReadReplicaInfo readReplicaInfo, WritableChannel channel ) throws IOException
    {
        clientConnectorAddressesMarshal.marshal( readReplicaInfo.connectors(), channel );
        advertisedSocketAddressMarshal.marshal( readReplicaInfo.getCatchupServer(), channel );
        marshalGroups( readReplicaInfo, channel );
        marshalDatabaseIds( readReplicaInfo, channel );
    }
}
