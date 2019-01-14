/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.BooleanMarshal;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class CoreTopologyMarshal extends SafeChannelMarshal<CoreTopology>
{
    private final ChannelMarshal<MemberId> memberIdMarshal = new MemberId.Marshal();
    private final ChannelMarshal<CoreServerInfo> coreServerInfoChannelMarshal = new CoreServerInfoMarshal();
    private final ChannelMarshal<ClusterId> clusterIdMarshal = new ClusterId.Marshal();

    @Override
    protected CoreTopology unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        ClusterId clusterId = clusterIdMarshal.unmarshal( channel );
        boolean canBeBootstrapped = BooleanMarshal.unmarshal( channel );

        int memberCount = channel.getInt();
        HashMap<MemberId,CoreServerInfo> members = new HashMap<>( memberCount );
        for ( int i = 0; i < memberCount; i++ )
        {
            MemberId memberId = memberIdMarshal.unmarshal( channel );
            CoreServerInfo coreServerInfo = coreServerInfoChannelMarshal.unmarshal( channel );
            members.put( memberId, coreServerInfo );
        }

        return new CoreTopology( clusterId, canBeBootstrapped, members );
    }

    @Override
    public void marshal( CoreTopology coreTopology, WritableChannel channel ) throws IOException
    {
        clusterIdMarshal.marshal( coreTopology.clusterId(), channel );
        BooleanMarshal.marshal( channel, coreTopology.canBeBootstrapped() );

        channel.putInt( coreTopology.members().size() );
        for ( Map.Entry<MemberId,CoreServerInfo> entry : coreTopology.members().entrySet() )
        {
            memberIdMarshal.marshal( entry.getKey(), channel );
            coreServerInfoChannelMarshal.marshal( entry.getValue(), channel );
        }
    }
}
