/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.emptyMap;

public class CoreTopology implements Topology<CoreServerInfo>
{
    public static final CoreTopology EMPTY = new CoreTopology( null, null, false, emptyMap() );

    private final DatabaseId databaseId;
    private final ClusterId clusterId;
    private final boolean canBeBootstrapped;
    private final Map<MemberId,CoreServerInfo> coreMembers;

    public CoreTopology( DatabaseId databaseId, ClusterId clusterId, boolean canBeBootstrapped, Map<MemberId,CoreServerInfo> coreMembers )
    {
        this.databaseId = databaseId;
        this.clusterId = clusterId;
        this.canBeBootstrapped = canBeBootstrapped;
        this.coreMembers = Map.copyOf( coreMembers );
    }

    @Override
    public Map<MemberId,CoreServerInfo> members()
    {
        return coreMembers;
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public boolean canBeBootstrapped()
    {
        return canBeBootstrapped;
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
        CoreTopology that = (CoreTopology) o;
        return canBeBootstrapped == that.canBeBootstrapped &&
               Objects.equals( databaseId, that.databaseId ) &&
               Objects.equals( clusterId, that.clusterId ) &&
               Objects.equals( coreMembers, that.coreMembers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, clusterId, canBeBootstrapped, coreMembers );
    }

    @Override
    public String toString()
    {
        return "CoreTopology{" +
               "databaseId='" + databaseId + '\'' +
               ", clusterId=" + clusterId +
               ", canBeBootstrapped=" + canBeBootstrapped +
               ", coreMembers=" + coreMembers +
               '}';
    }
}
