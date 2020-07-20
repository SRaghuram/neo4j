/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import com.neo4j.causalclustering.discovery.CoreServerInfo;

import java.util.Objects;

import org.neo4j.dbms.identity.ServerId;

public class CoreServerInfoForServerId
{
    private final ServerId serverId;
    private final CoreServerInfo coreServerInfo;

    public CoreServerInfoForServerId( ServerId serverId, CoreServerInfo coreServerInfo )
    {
        this.serverId = serverId;
        this.coreServerInfo = coreServerInfo;
    }

    public ServerId serverId()
    {
        return serverId;
    }

    public CoreServerInfo coreServerInfo()
    {
        return coreServerInfo;
    }

    @Override
    public String toString()
    {
        return "CoreServerInfoForServerId{" +
                "serverId=" + serverId +
                ", coreServerInfo=" + coreServerInfo +
                '}';
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
        CoreServerInfoForServerId that = (CoreServerInfoForServerId) o;
        return Objects.equals( serverId, that.serverId ) && Objects.equals( coreServerInfo, that.coreServerInfo );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( serverId, coreServerInfo );
    }
}
