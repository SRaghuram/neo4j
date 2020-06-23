/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForMemberId;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

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
