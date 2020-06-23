/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.kernel.database.DatabaseId;

public class CoreTopologyMarshal extends SafeChannelMarshal<DatabaseCoreTopology>
{
    private final ChannelMarshal<MemberId> memberIdMarshal = new MemberId.Marshal();
    private final ChannelMarshal<CoreServerInfo> coreServerInfoChannelMarshal = new CoreServerInfoMarshal();
    private final ChannelMarshal<RaftId> raftIdMarshal = new RaftId.Marshal();

    @Override
    protected DatabaseCoreTopology unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        RaftId raftId = raftIdMarshal.unmarshal( channel );

        int memberCount = channel.getInt();
        HashMap<MemberId,CoreServerInfo> members = new HashMap<>( memberCount );
        for ( int i = 0; i < memberCount; i++ )
        {
            MemberId memberId = memberIdMarshal.unmarshal( channel );
            CoreServerInfo coreServerInfo = coreServerInfoChannelMarshal.unmarshal( channel );
            members.put( memberId, coreServerInfo );
        }

        return new DatabaseCoreTopology( databaseId, raftId, members );
    }

    @Override
    public void marshal( DatabaseCoreTopology coreTopology, WritableChannel channel ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( coreTopology.databaseId(), channel );
        raftIdMarshal.marshal( coreTopology.raftId(), channel );

        channel.putInt( coreTopology.members().size() );
        for ( Map.Entry<MemberId,CoreServerInfo> entry : coreTopology.members().entrySet() )
        {
            MemberId memberId = entry.getKey();
            CoreServerInfo coreServerInfo = entry.getValue();
            memberIdMarshal.marshal( memberId, channel );
            coreServerInfoChannelMarshal.marshal( coreServerInfo, channel );
        }
    }
}
