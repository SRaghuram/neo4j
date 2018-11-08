/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.membership;

import java.io.IOException;
import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

public class MemberIdSet implements RaftGroup<MemberId>
{
    private final Set<MemberId> members;

    public MemberIdSet( Set<MemberId> members )
    {
        this.members = members;
    }

    @Override
    public String toString()
    {
        return "MemberIdSet{ members=" + members + '}';
    }

    @Override
    public Set<MemberId> getMembers()
    {
        return members;
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

        MemberIdSet that = (MemberIdSet) o;

        return !(members != null ? !members.equals( that.members ) : that.members != null);

    }

    @Override
    public int hashCode()
    {
        return members != null ? members.hashCode() : 0;
    }

    @Override
    public void dispatch( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }
}
