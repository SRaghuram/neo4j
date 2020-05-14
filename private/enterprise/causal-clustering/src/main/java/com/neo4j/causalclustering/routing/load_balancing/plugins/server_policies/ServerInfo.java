/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.core.ServerGroupName;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Objects;
import java.util.Set;

import org.neo4j.configuration.helpers.SocketAddress;

/**
 * Hold the server information that is interesting for load balancing purposes.
 */
public class ServerInfo
{
    private final SocketAddress boltAddress;
    private MemberId memberId;
    private Set<ServerGroupName> groups;

    public ServerInfo( SocketAddress boltAddress, MemberId memberId, Set<ServerGroupName> groups )
    {
        this.boltAddress = boltAddress;
        this.memberId = memberId;
        this.groups = groups;
    }

    public MemberId memberId()
    {
        return memberId;
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
        return Objects.equals( boltAddress, that.boltAddress ) && Objects.equals( memberId, that.memberId ) &&
                Objects.equals( groups, that.groups );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( boltAddress, memberId, groups );
    }

    @Override
    public String toString()
    {
        return "ServerInfo{" + "boltAddress=" + boltAddress + ", memberId=" + memberId + ", groups=" + groups + '}';
    }
}
