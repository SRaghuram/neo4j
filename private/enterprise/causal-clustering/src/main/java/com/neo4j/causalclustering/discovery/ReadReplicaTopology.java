/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.emptyMap;

// todo: consider renaming to DatabaseReadReplicaTopology
public class ReadReplicaTopology implements Topology<ReadReplicaInfo>
{
    public static final ReadReplicaTopology EMPTY = new ReadReplicaTopology( null, emptyMap() );

    private final DatabaseId databaseId;
    private final Map<MemberId,ReadReplicaInfo> readReplicaMembers;

    public ReadReplicaTopology( DatabaseId databaseId, Map<MemberId,ReadReplicaInfo> readReplicaMembers )
    {
        this.databaseId = databaseId;
        this.readReplicaMembers = readReplicaMembers;
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
        ReadReplicaTopology that = (ReadReplicaTopology) o;
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
        return "ReadReplicaTopology{" +
               "databaseId='" + databaseId + '\'' +
               ", readReplicaMembers=" + readReplicaMembers +
               '}';
    }
}
