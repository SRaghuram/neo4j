/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.kernel.database.DatabaseId;

public class DatabaseLeaderInfoMessageMarshal extends SafeChannelMarshal<LeaderInfoDirectoryMessage>
{
    private final ChannelMarshal<LeaderInfo> leaderInfoMarshal = new LeaderInfoMarshal();

    @Override
    protected LeaderInfoDirectoryMessage unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        int size = channel.getInt();
        HashMap<DatabaseId,LeaderInfo> leaders = new HashMap<>( size );
        for ( int i = 0; i < size; i++ )
        {
            DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
            LeaderInfo leaderInfo = leaderInfoMarshal.unmarshal( channel );
            leaders.put( databaseId, leaderInfo );
        }
        return new LeaderInfoDirectoryMessage( leaders );
    }

    @Override
    public void marshal( LeaderInfoDirectoryMessage leaderInfoDirectoryMessage, WritableChannel channel ) throws IOException
    {
        channel.putInt( leaderInfoDirectoryMessage.leaders().size() );
        for ( Map.Entry<DatabaseId,LeaderInfo> entry : leaderInfoDirectoryMessage.leaders().entrySet() )
        {
            DatabaseId databaseId = entry.getKey();
            LeaderInfo leaderInfo = entry.getValue();
            DatabaseIdWithoutNameMarshal.INSTANCE.marshal( databaseId, channel );
            leaderInfoMarshal.marshal( leaderInfo, channel );
        }
    }
}
