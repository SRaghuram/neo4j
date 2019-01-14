/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.BooleanMarshal;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class CoreServerInfoMarshal extends SafeChannelMarshal<CoreServerInfo>
{
    private final ChannelMarshal<ClientConnectorAddresses> clientConnectorAddressesMarshal = new ClientConnectorAddresses.Marshal();
    private final ChannelMarshal<AdvertisedSocketAddress> advertisedSocketAddressMarshal = new AdvertisedSocketAddressMarshal();

    @Override
    protected CoreServerInfo unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        AdvertisedSocketAddress raftServer = advertisedSocketAddressMarshal.unmarshal( channel );
        AdvertisedSocketAddress catchupServer = advertisedSocketAddressMarshal.unmarshal( channel );
        ClientConnectorAddresses clientConnectorAddresses = clientConnectorAddressesMarshal.unmarshal( channel );
        int groupsSize = channel.getInt();
        Set<String> groups = new HashSet<>( groupsSize );
        for ( int i = 0; i < groupsSize; i++ )
        {
            groups.add( StringMarshal.unmarshal( channel ) );
        }
        String databaseName = StringMarshal.unmarshal( channel );
        boolean refuseToBeLeader = BooleanMarshal.unmarshal( channel );
        return new CoreServerInfo( raftServer, catchupServer, clientConnectorAddresses, groups, databaseName, refuseToBeLeader );
    }

    @Override
    public void marshal( CoreServerInfo coreServerInfo, WritableChannel channel ) throws IOException
    {
        advertisedSocketAddressMarshal.marshal( coreServerInfo.getRaftServer(), channel );
        advertisedSocketAddressMarshal.marshal( coreServerInfo.getCatchupServer(), channel );
        clientConnectorAddressesMarshal.marshal( coreServerInfo.connectors(), channel );
        channel.putInt( coreServerInfo.groups().size() );
        for ( String group : coreServerInfo.groups() )
        {
            StringMarshal.marshal( channel, group );
        }
        StringMarshal.marshal( channel, coreServerInfo.getDatabaseName() );
        BooleanMarshal.marshal( channel, coreServerInfo.refusesToBeLeader() );
    }
}
