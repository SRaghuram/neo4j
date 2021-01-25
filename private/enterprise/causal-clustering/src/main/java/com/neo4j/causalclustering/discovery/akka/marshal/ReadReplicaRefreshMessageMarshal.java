/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorRef;
import akka.actor.ExtendedActorSystem;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRefreshMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaRefreshMessageMarshal extends SafeChannelMarshal<ReadReplicaRefreshMessage>
{
    private final ChannelMarshal<ReadReplicaInfo> readReplicaInfoMarshal;
    private final ChannelMarshal<ServerId> serverIdMarshal = ServerId.Marshal.INSTANCE;
    private final ChannelMarshal<ActorRef> actorRefMarshal;

    public ReadReplicaRefreshMessageMarshal( ExtendedActorSystem system )
    {
        this.actorRefMarshal = new ActorRefMarshal( system );
        readReplicaInfoMarshal = new ReadReplicaInfoMarshal();
    }

    @Override
    protected ReadReplicaRefreshMessage unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var rrInfo = readReplicaInfoMarshal.unmarshal( channel );
        var serverId = serverIdMarshal.unmarshal( channel );
        var clusterClient = actorRefMarshal.unmarshal( channel );
        var topologyClient = actorRefMarshal.unmarshal( channel );

        var databaseStates = new HashMap<DatabaseId,DiscoveryDatabaseState>();
        int size = channel.getInt();
        for ( int i = 0; i < size; i++ )
        {
            var id = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
            var state = DiscoveryDatabaseStateMarshal.INSTANCE.unmarshal( channel );
            databaseStates.put( id, state );
        }

        return new ReadReplicaRefreshMessage( rrInfo, serverId, clusterClient, topologyClient, databaseStates );
    }

    @Override
    public void marshal( ReadReplicaRefreshMessage readReplicaRefreshMessage, WritableChannel channel ) throws IOException
    {
        readReplicaInfoMarshal.marshal( readReplicaRefreshMessage.readReplicaInfo(), channel );
        serverIdMarshal.marshal( readReplicaRefreshMessage.serverId(), channel );
        actorRefMarshal.marshal( readReplicaRefreshMessage.clusterClientManager(), channel );
        actorRefMarshal.marshal( readReplicaRefreshMessage.topologyClientActorRef(), channel );

        var databaseStates = readReplicaRefreshMessage.databaseStates();
        channel.putInt( databaseStates.size() );
        for ( Map.Entry<DatabaseId,DiscoveryDatabaseState> entry : databaseStates.entrySet() )
        {
            DatabaseIdWithoutNameMarshal.INSTANCE.marshal( entry.getKey(), channel );
            DiscoveryDatabaseStateMarshal.INSTANCE.marshal( entry.getValue(), channel );
        }
    }
}
