/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.discovery.akka.database.state.ReplicatedDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.BooleanMarshal;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public class ReplicatedDatabaseStateMarshal extends SafeChannelMarshal<ReplicatedDatabaseState>
{
    public static ReplicatedDatabaseStateMarshal INSTANCE = new ReplicatedDatabaseStateMarshal();

    private ReplicatedDatabaseStateMarshal()
    {
    }

    @Override
    protected ReplicatedDatabaseState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var databaseId = DatabaseIdMarshal.INSTANCE.unmarshal( channel );
        var containsCoreMembers = BooleanMarshal.unmarshal( channel );
        var memberCount = channel.getInt();

        HashMap<MemberId,DatabaseState> memberStates = new HashMap<>();
        for ( int i = 0; i < memberCount; i++ )
        {
            var memberId = MemberId.Marshal.INSTANCE.unmarshal( channel );
            var databaseState = DatabaseStateMarshal.INSTANCE.unmarshal( channel );
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
        DatabaseIdMarshal.INSTANCE.marshal( clusteredDatabaseState.databaseId(), channel );
        BooleanMarshal.marshal( channel, clusteredDatabaseState.containsCoreStates() );
        var memberStates = Map.copyOf( clusteredDatabaseState.memberStates() );
        channel.putInt( memberStates.size() );

        for ( Map.Entry<MemberId,DatabaseState> entry : memberStates.entrySet() )
        {
            var memberId = entry.getKey();
            var databaseState = entry.getValue();
            MemberId.Marshal.INSTANCE.marshal( memberId, channel );
            DatabaseStateMarshal.INSTANCE.marshal( databaseState, channel );
        }
    }
}
