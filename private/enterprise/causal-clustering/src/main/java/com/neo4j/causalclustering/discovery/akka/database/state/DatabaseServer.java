/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.database.state;

import java.util.Objects;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

/**
 * Represents a database hosted by a server.
 */
public class DatabaseServer
{
    private final DatabaseId databaseId;
    private final ServerId serverId;

    public DatabaseServer( DatabaseId databaseId, ServerId serverId )
    {
        this.databaseId = databaseId;
        this.serverId = serverId;
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public ServerId serverId()
    {
        return serverId;
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
        DatabaseServer that = (DatabaseServer) o;
        return Objects.equals( databaseId, that.databaseId ) && Objects.equals( serverId, that.serverId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, serverId );
    }

    @Override
    public String toString()
    {
        return "DatabaseServer{" + "databaseId=" + databaseId + ", serverId=" + serverId + '}';
    }
}
