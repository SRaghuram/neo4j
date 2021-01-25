/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.configuration.ServerGroupName;

import java.util.Objects;
import java.util.Set;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;

/**
 * Hold the server information that is interesting for load balancing purposes.
 */
public class ServerInfo
{
    private final SocketAddress boltAddress;
    private final ServerId serverId;
    private final Set<ServerGroupName> groups;

    public ServerInfo( SocketAddress boltAddress, ServerId serverId, Set<ServerGroupName> groups )
    {
        this.boltAddress = boltAddress;
        this.serverId = serverId;
        this.groups = groups;
    }

    public ServerId serverId()
    {
        return serverId;
    }

    SocketAddress boltAddress()
    {
        return boltAddress;
    }

    Set<ServerGroupName> groups()
    {
        return groups;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ServerInfo that = (ServerInfo) o;
        return Objects.equals( boltAddress, that.boltAddress ) && Objects.equals( serverId, that.serverId ) &&
                Objects.equals( groups, that.groups );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( boltAddress, serverId, groups );
    }

    @Override
    public String toString()
    {
        return "ServerInfo{" + "boltAddress=" + boltAddress + ", serverId=" + serverId + ", groups=" + groups + '}';
    }
}
