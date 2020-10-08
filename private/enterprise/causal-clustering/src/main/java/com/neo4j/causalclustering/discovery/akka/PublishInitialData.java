/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.member.DiscoveryMember;

import java.util.Objects;

public final class PublishInitialData
{
    private final DiscoveryMember memberSnapshot;

    public PublishInitialData( DiscoveryMember memberSnapshot )
    {
        this.memberSnapshot = memberSnapshot;
    }

    public DiscoveryMember getSnapshot()
    {
        return memberSnapshot;
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
        PublishInitialData that = (PublishInitialData) o;
        return Objects.equals( memberSnapshot, that.memberSnapshot );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( memberSnapshot );
    }
}
