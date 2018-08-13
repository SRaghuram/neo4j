/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import java.io.IOException;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForMemberId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class CoreServerInfoForMemberIdMarshal extends SafeChannelMarshal<CoreServerInfoForMemberId>
{
    private final ChannelMarshal<MemberId> memberIdMarshal = new MemberId.Marshal();
    private final ChannelMarshal<CoreServerInfo> coreServerInfoMarshal = new CoreServerInfoMarshal();

    @Override
    protected CoreServerInfoForMemberId unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        MemberId memberId = memberIdMarshal.unmarshal( channel );
        CoreServerInfo coreServerInfo = coreServerInfoMarshal.unmarshal( channel );
        return new CoreServerInfoForMemberId( memberId, coreServerInfo );
    }

    @Override
    public void marshal( CoreServerInfoForMemberId coreServerInfoForMemberId, WritableChannel channel ) throws IOException
    {
        memberIdMarshal.marshal( coreServerInfoForMemberId.memberId(), channel );
        coreServerInfoMarshal.marshal( coreServerInfoForMemberId.coreServerInfo(), channel );
    }
}
