/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.kernel.database.DatabaseId;

public class ReplicatedRaftMappingMarshal extends SafeChannelMarshal<ReplicatedRaftMapping>
{
    public static final ReplicatedRaftMappingMarshal INSTANCE = new ReplicatedRaftMappingMarshal();

    private ReplicatedRaftMappingMarshal()
    {
    }

    @Override
    protected ReplicatedRaftMapping unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var serverId = ServerId.Marshal.INSTANCE.unmarshal( channel );
        var memberCount = channel.getInt();

        var mapping = new HashMap<DatabaseId,RaftMemberId>();
        for ( var i = 0; i < memberCount; i++ )
        {
            var databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
            var raftMemberId = RaftMemberId.Marshal.INSTANCE.unmarshal( channel );
            mapping.put( databaseId, raftMemberId );
        }
        return ReplicatedRaftMapping.of( serverId, mapping );
    }

    @Override
    public void marshal( ReplicatedRaftMapping replicatedRaftMapping, WritableChannel channel ) throws IOException
    {
        ServerId.Marshal.INSTANCE.marshal( replicatedRaftMapping.serverId(), channel );
        var mapping = Map.copyOf( replicatedRaftMapping.mapping() );
        channel.putInt( mapping.size() );

        for ( var entry : mapping.entrySet() )
        {
            var databaseId = entry.getKey();
            var raftMemberId = entry.getValue();
            DatabaseIdWithoutNameMarshal.INSTANCE.marshal( databaseId, channel );
            RaftMemberId.Marshal.INSTANCE.marshal( raftMemberId, channel );
        }
    }
}
