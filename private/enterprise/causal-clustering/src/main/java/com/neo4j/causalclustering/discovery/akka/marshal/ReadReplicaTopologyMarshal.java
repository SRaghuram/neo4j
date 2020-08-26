/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaTopologyMarshal extends SafeChannelMarshal<DatabaseReadReplicaTopology>
{
    private final ChannelMarshal<ReadReplicaInfo> readReplicaInfoMarshal;
    private final ChannelMarshal<MemberId> memberIdMarshal = new MemberId.Marshal();

    public ReadReplicaTopologyMarshal()
    {
        this.readReplicaInfoMarshal = new ReadReplicaInfoMarshal();
    }

    @Override
    protected DatabaseReadReplicaTopology unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        int size = channel.getInt();
        HashMap<MemberId,ReadReplicaInfo> replicas = new HashMap<>( size );
        for ( int i = 0; i < size; i++ )
        {
            MemberId memberId = memberIdMarshal.unmarshal( channel );
            ReadReplicaInfo readReplicaInfo = readReplicaInfoMarshal.unmarshal( channel );
            replicas.put( memberId, readReplicaInfo );
        }

        return new DatabaseReadReplicaTopology( databaseId, replicas );
    }

    @Override
    public void marshal( DatabaseReadReplicaTopology readReplicaTopology, WritableChannel channel ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( readReplicaTopology.databaseId(), channel );
        channel.putInt( readReplicaTopology.servers().size() );
        for ( Map.Entry<MemberId,ReadReplicaInfo> entry : readReplicaTopology.servers().entrySet() )
        {
            MemberId memberId = entry.getKey();
            ReadReplicaInfo readReplicaInfo = entry.getValue();
            memberIdMarshal.marshal( memberId, channel );
            readReplicaInfoMarshal.marshal( readReplicaInfo, channel );
        }
    }
}
