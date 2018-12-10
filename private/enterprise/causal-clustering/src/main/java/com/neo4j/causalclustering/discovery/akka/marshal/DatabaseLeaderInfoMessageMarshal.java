/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class DatabaseLeaderInfoMessageMarshal extends SafeChannelMarshal<LeaderInfoDirectoryMessage>
{
    private final ChannelMarshal<LeaderInfo> leaderInfoMarshal = new LeaderInfoMarshal();

    @Override
    protected LeaderInfoDirectoryMessage unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        int size = channel.getInt();
        HashMap<String,LeaderInfo> leaders = new HashMap<>( size );
        for ( int i = 0; i < size; i++ )
        {
            String database = StringMarshal.unmarshal( channel );
            LeaderInfo leaderInfo = leaderInfoMarshal.unmarshal( channel );
            leaders.put( database, leaderInfo );
        }
        return new LeaderInfoDirectoryMessage( leaders );
    }

    @Override
    public void marshal( LeaderInfoDirectoryMessage leaderInfoDirectoryMessage, WritableChannel channel ) throws IOException
    {
        channel.putInt( leaderInfoDirectoryMessage.leaders().size() );
        for ( Map.Entry<String,LeaderInfo> entry : leaderInfoDirectoryMessage.leaders().entrySet() )
        {
            String database = entry.getKey();
            LeaderInfo leaderInfo = entry.getValue();
            StringMarshal.marshal( channel, database );
            leaderInfoMarshal.marshal( leaderInfo, channel );
        }
    }
}
