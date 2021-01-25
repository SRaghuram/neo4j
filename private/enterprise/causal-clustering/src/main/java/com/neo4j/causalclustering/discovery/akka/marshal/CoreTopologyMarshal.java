/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.identity.RaftGroupId;

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

public class CoreTopologyMarshal extends SafeChannelMarshal<DatabaseCoreTopology>
{
    private final ChannelMarshal<ServerId> serverIdMarshal = ServerId.Marshal.INSTANCE;
    private final ChannelMarshal<CoreServerInfo> coreServerInfoChannelMarshal = new CoreServerInfoMarshal();
    private final ChannelMarshal<RaftGroupId> raftGroupIdMarshal = RaftGroupId.Marshal.INSTANCE;

    @Override
    protected DatabaseCoreTopology unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        RaftGroupId raftGroupId = raftGroupIdMarshal.unmarshal( channel );

        int memberCount = channel.getInt();
        HashMap<ServerId,CoreServerInfo> members = new HashMap<>( memberCount );
        for ( int i = 0; i < memberCount; i++ )
        {
            ServerId serverId = serverIdMarshal.unmarshal( channel );
            CoreServerInfo coreServerInfo = coreServerInfoChannelMarshal.unmarshal( channel );
            members.put( serverId, coreServerInfo );
        }

        return new DatabaseCoreTopology( databaseId, raftGroupId, members );
    }

    @Override
    public void marshal( DatabaseCoreTopology coreTopology, WritableChannel channel ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( coreTopology.databaseId(), channel );
        raftGroupIdMarshal.marshal( coreTopology.raftGroupId(), channel );

        channel.putInt( coreTopology.servers().size() );
        for ( Map.Entry<ServerId,CoreServerInfo> entry : coreTopology.servers().entrySet() )
        {
            ServerId serverId = entry.getKey();
            CoreServerInfo coreServerInfo = entry.getValue();
            serverIdMarshal.marshal( serverId, channel );
            coreServerInfoChannelMarshal.marshal( coreServerInfo, channel );
        }
    }
}
