/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.configuration.ServerGroupName;

import java.io.IOException;
import java.util.Set;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaInfoMarshal extends DiscoveryServerInfoMarshal<ReadReplicaInfo>
{
    private final ChannelMarshal<ConnectorAddresses> clientConnectorAddressesMarshal = new ConnectorAddresses.Marshal();
    private final ChannelMarshal<SocketAddress> advertisedSocketAddressMarshal = new AdvertisedSocketAddressMarshal();

    @Override
    protected ReadReplicaInfo unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        ConnectorAddresses connectorAddresses = clientConnectorAddressesMarshal.unmarshal( channel );
        SocketAddress catchupServer = advertisedSocketAddressMarshal.unmarshal( channel );
        Set<ServerGroupName> groups = unmarshalGroups( channel );
        Set<DatabaseId> databaseIds = unmarshalDatabaseIds( channel );
        return new ReadReplicaInfo( connectorAddresses, catchupServer, groups, databaseIds );
    }

    @Override
    public void marshal( ReadReplicaInfo readReplicaInfo, WritableChannel channel ) throws IOException
    {
        clientConnectorAddressesMarshal.marshal( readReplicaInfo.connectors(), channel );
        advertisedSocketAddressMarshal.marshal( readReplicaInfo.catchupServer(), channel );
        marshalGroups( readReplicaInfo, channel );
        marshalDatabaseIds( readReplicaInfo, channel );
    }
}
