/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

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
