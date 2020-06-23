/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.marshalling.BooleanMarshal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

public class ReplicatedDatabaseStateMarshal extends SafeChannelMarshal<ReplicatedDatabaseState>
{
    public static final ReplicatedDatabaseStateMarshal INSTANCE = new ReplicatedDatabaseStateMarshal();

    private ReplicatedDatabaseStateMarshal()
    {
    }

    @Override
    protected ReplicatedDatabaseState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        var containsCoreMembers = BooleanMarshal.unmarshal( channel );
        var memberCount = channel.getInt();

        HashMap<MemberId,DiscoveryDatabaseState> memberStates = new HashMap<>();
        for ( int i = 0; i < memberCount; i++ )
        {
            var memberId = MemberId.Marshal.INSTANCE.unmarshal( channel );
            var databaseState = DiscoveryDatabaseStateMarshal.INSTANCE.unmarshal( channel );
            memberStates.put( memberId, databaseState );
        }

        if ( containsCoreMembers )
        {
            return ReplicatedDatabaseState.ofCores( databaseId, memberStates );
        }
        return ReplicatedDatabaseState.ofReadReplicas( databaseId, memberStates );
    }

    @Override
    public void marshal( ReplicatedDatabaseState clusteredDatabaseState, WritableChannel channel ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( clusteredDatabaseState.databaseId(), channel );
        BooleanMarshal.marshal( channel, clusteredDatabaseState.containsCoreStates() );
        var memberStates = Map.copyOf( clusteredDatabaseState.memberStates() );
        channel.putInt( memberStates.size() );

        for ( Map.Entry<MemberId,DiscoveryDatabaseState> entry : memberStates.entrySet() )
        {
            var memberId = entry.getKey();
            var databaseState = entry.getValue();
            MemberId.Marshal.INSTANCE.marshal( memberId, channel );
            DiscoveryDatabaseStateMarshal.INSTANCE.marshal( databaseState, channel );
        }
    }
}
