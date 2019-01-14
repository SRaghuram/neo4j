/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import java.util.Objects;

import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.identity.MemberId;

public class CoreServerInfoForMemberId
{
    private final MemberId memberId;
    private final CoreServerInfo coreServerInfo;

    public CoreServerInfoForMemberId( MemberId memberId, CoreServerInfo coreServerInfo )
    {
        this.memberId = memberId;
        this.coreServerInfo = coreServerInfo;
    }

    public MemberId memberId()
    {
        return memberId;
    }

    public CoreServerInfo coreServerInfo()
    {
        return coreServerInfo;
    }

    @Override
    public String toString()
    {
        return "CoreServerInfoForMemberId{" +
                "memberId=" + memberId +
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
        CoreServerInfoForMemberId that = (CoreServerInfoForMemberId) o;
        return Objects.equals( memberId, that.memberId ) && Objects.equals( coreServerInfo, that.coreServerInfo );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( memberId, coreServerInfo );
    }
}
