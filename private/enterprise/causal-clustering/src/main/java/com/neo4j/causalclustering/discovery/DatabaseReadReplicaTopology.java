/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Objects;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class DatabaseReadReplicaTopology implements Topology<ReadReplicaInfo>
{
    private final DatabaseId databaseId;
    private final Map<MemberId,ReadReplicaInfo> readReplicaMembers;

    public DatabaseReadReplicaTopology( DatabaseId databaseId, Map<MemberId,ReadReplicaInfo> readReplicaMembers )
    {
        this.databaseId = requireNonNull( databaseId );
        this.readReplicaMembers = readReplicaMembers;
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
    public Map<MemberId, ReadReplicaInfo> members()
    {
        return readReplicaMembers;
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
               Objects.equals( readReplicaMembers, that.readReplicaMembers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, readReplicaMembers );
    }

    @Override
    public String toString()
    {
        return "DatabaseReadReplicaTopology{" +
               "databaseId='" + databaseId + '\'' +
               ", readReplicaMembers=" + readReplicaMembers +
               '}';
    }
}
