/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.Map;
import java.util.Objects;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class DatabaseReadReplicaTopology implements Topology<ReadReplicaInfo>
{
    private final DatabaseId databaseId;
    private final Map<ServerId,ReadReplicaInfo> readReplicaServers;

    public DatabaseReadReplicaTopology( DatabaseId databaseId, Map<ServerId,ReadReplicaInfo> readReplicaServers )
    {
        this.databaseId = requireNonNull( databaseId );
        this.readReplicaServers = readReplicaServers;
    }

    public static DatabaseReadReplicaTopology empty( DatabaseId databaseId )
    {
        return new DatabaseReadReplicaTopology( databaseId, emptyMap() );
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }

    @Override
    public Map<ServerId, ReadReplicaInfo> servers()
    {
        return readReplicaServers;
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
        var that = (DatabaseReadReplicaTopology) o;
        return Objects.equals( databaseId, that.databaseId ) &&
               Objects.equals( readReplicaServers, that.readReplicaServers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, readReplicaServers );
    }

    @Override
    public String toString()
    {
        return format( "DatabaseReadReplicaTopology{%s %s}", databaseId, readReplicaServers.keySet() );
    }
}
