/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public class LeaderInfoMarshal extends SafeChannelMarshal<LeaderInfo>
{
    private MemberId.Marshal memberIdMarshal = new MemberId.Marshal();

    @Override
    protected LeaderInfo unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        MemberId memberId = memberIdMarshal.unmarshal( channel );
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
