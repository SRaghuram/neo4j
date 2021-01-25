/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;

class ServerContext
{
    private final SocketAddress boltAddress;
    private final SocketAddress catchupAddress;
    private final ServerId serverId;

    static ServerContext local( ServerId serverId, SocketAddress boltAddress )
    {
        return new ServerContext( serverId, boltAddress, null );
    }

    static ServerContext remote( ServerId serverId, DiscoveryServerInfo info )
    {
        return new ServerContext( serverId, info.boltAddress(), info.catchupServer() );
    }

    private ServerContext( ServerId serverId, SocketAddress boltAddress, SocketAddress catchupAddress )
    {
        this.serverId = serverId;
        this.boltAddress = boltAddress;
        this.catchupAddress = catchupAddress;
    }

    public ServerId serverId()
    {
        return serverId;
    }

    public SocketAddress boltAddress()
    {
        return boltAddress;
    }

    public SocketAddress catchupAddress()
    {
        return catchupAddress;
    }
}
