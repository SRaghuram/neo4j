/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.messaging.marshalling.BooleanMarshal;
import com.neo4j.configuration.ServerGroupName;

import java.io.IOException;
import java.util.Set;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.kernel.database.DatabaseId;

public class CoreServerInfoMarshal extends DiscoveryServerInfoMarshal<CoreServerInfo>
{
    private final ChannelMarshal<ConnectorAddresses> clientConnectorAddressesMarshal = new ConnectorAddresses.Marshal();
    private final ChannelMarshal<SocketAddress> advertisedSocketAddressMarshal = new AdvertisedSocketAddressMarshal();

    @Override
    protected CoreServerInfo unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        SocketAddress raftServer = advertisedSocketAddressMarshal.unmarshal( channel );
        SocketAddress catchupServer = advertisedSocketAddressMarshal.unmarshal( channel );
        ConnectorAddresses connectorAddresses = clientConnectorAddressesMarshal.unmarshal( channel );
        Set<ServerGroupName> groups = unmarshalGroups( channel );
        Set<DatabaseId> databaseIds = unmarshalDatabaseIds( channel );
        BooleanMarshal.unmarshal( channel ); // result of this ignored as refuse to be a leader is no longer supported

        return new CoreServerInfo( raftServer, catchupServer, connectorAddresses, groups, databaseIds );
    }

    @Override
    public void marshal( CoreServerInfo coreServerInfo, WritableChannel channel ) throws IOException
    {
        advertisedSocketAddressMarshal.marshal( coreServerInfo.getRaftServer(), channel );
        advertisedSocketAddressMarshal.marshal( coreServerInfo.catchupServer(), channel );
        clientConnectorAddressesMarshal.marshal( coreServerInfo.connectors(), channel );
        marshalGroups( coreServerInfo, channel );
        marshalDatabaseIds( coreServerInfo, channel );
        var refuseToBeLeader = false; //Setting no longer supported. Remains in marshal for compat.
        BooleanMarshal.marshal( channel, refuseToBeLeader );
    }
}
