/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.database.DatabaseId;

public class CoreTopologyMarshal extends SafeChannelMarshal<DatabaseCoreTopology>
{
    private final ChannelMarshal<MemberId> memberIdMarshal = new MemberId.Marshal();
    private final ChannelMarshal<CoreServerInfo> coreServerInfoChannelMarshal = new CoreServerInfoMarshal();
    private final ChannelMarshal<ClusterId> clusterIdMarshal = new ClusterId.Marshal();

    @Override
    protected DatabaseCoreTopology unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdMarshal.INSTANCE.unmarshal( channel );
        ClusterId clusterId = clusterIdMarshal.unmarshal( channel );

        int memberCount = channel.getInt();
        HashMap<MemberId,CoreServerInfo> members = new HashMap<>( memberCount );
        for ( int i = 0; i < memberCount; i++ )
        {
            MemberId memberId = memberIdMarshal.unmarshal( channel );
            CoreServerInfo coreServerInfo = coreServerInfoChannelMarshal.unmarshal( channel );
            members.put( memberId, coreServerInfo );
        }

        return new DatabaseCoreTopology( databaseId, clusterId, members );
    }

    @Override
    public void marshal( DatabaseCoreTopology coreTopology, WritableChannel channel ) throws IOException
    {
        DatabaseIdMarshal.INSTANCE.marshal( coreTopology.databaseId(), channel );
        clusterIdMarshal.marshal( coreTopology.clusterId(), channel );

        channel.putInt( coreTopology.members().size() );
        for ( Map.Entry<MemberId,CoreServerInfo> entry : coreTopology.members().entrySet() )
        {
            memberIdMarshal.marshal( entry.getKey(), channel );
            coreServerInfoChannelMarshal.marshal( entry.getValue(), channel );
        }
    }
}
