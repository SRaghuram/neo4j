/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReadReplicaTopologyMarshal extends SafeChannelMarshal<ReadReplicaTopology>
{
    private final ChannelMarshal<ReadReplicaInfo> readReplicaInfoMarshal = new ReadReplicaInfoMarshal();
    private final ChannelMarshal<MemberId> memberIdMarshal = new MemberId.Marshal();

    @Override
    protected ReadReplicaTopology unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        int size = channel.getInt();
        HashMap<MemberId,ReadReplicaInfo> replicas = new HashMap<>( size );
        for ( int i = 0; i < size; i++ )
        {
            MemberId memberId = memberIdMarshal.unmarshal( channel );
            ReadReplicaInfo readReplicaInfo = readReplicaInfoMarshal.unmarshal( channel );
            replicas.put( memberId, readReplicaInfo );
        }
        return new ReadReplicaTopology( replicas );
    }

    @Override
    public void marshal( ReadReplicaTopology readReplicaTopology, WritableChannel channel ) throws IOException
    {
        channel.putInt( readReplicaTopology.members().size() );
        for ( Map.Entry<MemberId,ReadReplicaInfo> entry : readReplicaTopology.members().entrySet() )
        {
            MemberId memberId = entry.getKey();
            ReadReplicaInfo readReplicaInfo = entry.getValue();
            memberIdMarshal.marshal( memberId, channel );
            readReplicaInfoMarshal.marshal( readReplicaInfo, channel );
        }
    }
}
