/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.neo4j.causalclustering.identity.MemberId;

import static java.util.Collections.emptyMap;

public class ReadReplicaTopology implements Topology<ReadReplicaInfo>
{
    public static final ReadReplicaTopology EMPTY = new ReadReplicaTopology( emptyMap() );

    private final Map<MemberId,ReadReplicaInfo> readReplicaMembers;

    public ReadReplicaTopology( Map<MemberId,ReadReplicaInfo> readReplicaMembers )
    {
        this.readReplicaMembers = readReplicaMembers;
    }

    public Collection<ReadReplicaInfo> allMemberInfo()
    {
        return readReplicaMembers.values();
    }

    @Override
    public Map<MemberId, ReadReplicaInfo> members()
    {
        return readReplicaMembers;
    }

    @Override
    public String toString()
    {
        return String.format( "{readReplicas=%s}", readReplicaMembers );
    }

    @Override
    public ReadReplicaTopology filterTopologyByDb( String dbName )
    {
        Map<MemberId, ReadReplicaInfo> filteredMembers = filterHostsByDb( members(), dbName );

        return new ReadReplicaTopology( filteredMembers );
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
        return Objects.equals( readReplicaMembers, that.readReplicaMembers );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( readReplicaMembers );
    }
}
