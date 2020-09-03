/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForServerId;

import java.io.IOException;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

public class CoreServerInfoForServerIdMarshal extends SafeChannelMarshal<CoreServerInfoForServerId>
{
    private final ChannelMarshal<ServerId> serverIdMarshal = ServerId.Marshal.INSTANCE;
    private final ChannelMarshal<CoreServerInfo> coreServerInfoMarshal = new CoreServerInfoMarshal();

    @Override
    protected CoreServerInfoForServerId unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var serverId = serverIdMarshal.unmarshal( channel );
        var coreServerInfo = coreServerInfoMarshal.unmarshal( channel );
        return new CoreServerInfoForServerId( serverId, coreServerInfo );
    }

    @Override
    public void marshal( CoreServerInfoForServerId coreServerInfoForServerId, WritableChannel channel ) throws IOException
    {
        serverIdMarshal.marshal( coreServerInfoForServerId.serverId(), channel );
        coreServerInfoMarshal.marshal( coreServerInfoForServerId.coreServerInfo(), channel );
    }
}
