/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaInfoMarshal extends SafeChannelMarshal<ReadReplicaInfo>
{
    private final ChannelMarshal<ClientConnectorAddresses> clientConnectorAddressesMarshal = new ClientConnectorAddresses.Marshal();
    private final ChannelMarshal<AdvertisedSocketAddress> advertisedSocketAddressMarshal = new AdvertisedSocketAddressMarshal();

    @Override
    protected ReadReplicaInfo unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        ClientConnectorAddresses clientConnectorAddresses = clientConnectorAddressesMarshal.unmarshal( channel );
        AdvertisedSocketAddress catchupServer = advertisedSocketAddressMarshal.unmarshal( channel );

        int groupsSize = channel.getInt();
        Set<String> groups = new HashSet<>( groupsSize );
        for ( int i = 0; i < groupsSize; i++ )
        {
            groups.add( StringMarshal.unmarshal( channel ) );
        }

        int databaseIdsSize = channel.getInt();
        Set<DatabaseId> databaseIds = new HashSet<>( groupsSize );
        for ( int i = 0; i < databaseIdsSize; i++ )
        {
            databaseIds.add( DatabaseIdMarshal.INSTANCE.unmarshal( channel ) );
        }

        return new ReadReplicaInfo( clientConnectorAddresses, catchupServer, groups, databaseIds );
    }

    @Override
    public void marshal( ReadReplicaInfo readReplicaInfo, WritableChannel channel ) throws IOException
    {
        clientConnectorAddressesMarshal.marshal( readReplicaInfo.connectors(), channel );
        advertisedSocketAddressMarshal.marshal( readReplicaInfo.getCatchupServer(), channel );

        channel.putInt( readReplicaInfo.groups().size() );
        for ( String group : readReplicaInfo.groups() )
        {
            StringMarshal.marshal( channel, group );
        }

        channel.putInt( readReplicaInfo.getDatabaseIds().size() );
        for ( DatabaseId databaseId : readReplicaInfo.getDatabaseIds() )
        {
            DatabaseIdMarshal.INSTANCE.marshal( databaseId, channel );
        }
    }
}
