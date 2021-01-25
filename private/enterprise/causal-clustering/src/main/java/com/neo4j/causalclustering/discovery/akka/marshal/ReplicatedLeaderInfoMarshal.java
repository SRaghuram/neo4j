/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.directory.ReplicatedLeaderInfo;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

public class ReplicatedLeaderInfoMarshal extends SafeChannelMarshal<ReplicatedLeaderInfo>
{

    private final LeaderInfoMarshal leaderInfoMarshal = new LeaderInfoMarshal();

    @Override
    protected ReplicatedLeaderInfo unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        return new ReplicatedLeaderInfo( leaderInfoMarshal.unmarshal( channel ) );
    }

    @Override
    public void marshal( ReplicatedLeaderInfo replicatedLeaderInfo, WritableChannel channel ) throws IOException
    {
        leaderInfoMarshal.marshal( replicatedLeaderInfo.leaderInfo(), channel );
    }
}
