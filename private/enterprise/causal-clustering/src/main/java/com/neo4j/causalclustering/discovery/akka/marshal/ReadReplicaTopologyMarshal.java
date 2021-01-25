/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;

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

public class ReadReplicaTopologyMarshal extends SafeChannelMarshal<DatabaseReadReplicaTopology>
{
    private final ChannelMarshal<ReadReplicaInfo> readReplicaInfoMarshal;
    private final ChannelMarshal<ServerId> serverIdMarshal = ServerId.Marshal.INSTANCE;

    public ReadReplicaTopologyMarshal()
    {
        this.readReplicaInfoMarshal = new ReadReplicaInfoMarshal();
    }

    @Override
    protected DatabaseReadReplicaTopology unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        int size = channel.getInt();
        HashMap<ServerId,ReadReplicaInfo> replicas = new HashMap<>( size );
        for ( int i = 0; i < size; i++ )
        {
            ServerId serverId = serverIdMarshal.unmarshal( channel );
            ReadReplicaInfo readReplicaInfo = readReplicaInfoMarshal.unmarshal( channel );
            replicas.put( serverId, readReplicaInfo );
        }

        return new DatabaseReadReplicaTopology( databaseId, replicas );
    }

    @Override
    public void marshal( DatabaseReadReplicaTopology readReplicaTopology, WritableChannel channel ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( readReplicaTopology.databaseId(), channel );
        channel.putInt( readReplicaTopology.servers().size() );
        for ( Map.Entry<ServerId,ReadReplicaInfo> entry : readReplicaTopology.servers().entrySet() )
        {
            ServerId serverId = entry.getKey();
            ReadReplicaInfo readReplicaInfo = entry.getValue();
            serverIdMarshal.marshal( serverId, channel );
            readReplicaInfoMarshal.marshal( readReplicaInfo, channel );
        }
    }
}
