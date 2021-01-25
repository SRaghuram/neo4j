/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

public class LeaderInfoMarshal extends SafeChannelMarshal<LeaderInfo>
{
    private RaftMemberId.Marshal memberIdMarshal = RaftMemberId.Marshal.INSTANCE;

    @Override
    protected LeaderInfo unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        RaftMemberId memberId = memberIdMarshal.unmarshal( channel );
        long term = channel.getLong();
        return new LeaderInfo( memberId, term );
    }

    @Override
    public void marshal( LeaderInfo leaderInfo, WritableChannel channel ) throws IOException
    {
        memberIdMarshal.marshal( leaderInfo.memberId(), channel );
        channel.putLong( leaderInfo.term() );
    }
}
