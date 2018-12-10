/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static java.lang.String.format;

public class RaftTestGroup implements RaftGroup<MemberId>
{
    private final Set<MemberId> members = new HashSet<>();

    public RaftTestGroup( Set<MemberId> members )
    {
        this.members.addAll( members );
    }

    public RaftTestGroup( int... memberIds )
    {
        for ( int memberId : memberIds )
        {
            this.members.add( member( memberId ) );
        }
    }

    public RaftTestGroup( MemberId... memberIds )
    {
        this.members.addAll( Arrays.asList( memberIds ) );
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

        RaftTestGroup that = (RaftTestGroup) o;

        return members.equals( that.members );

    }

    @Override
    public int hashCode()
    {
        return members.hashCode();
    }

    @Override
    public String toString()
    {
        return format( "RaftTestGroup{members=%s}", members );
    }

    @Override
    public void dispatch( ReplicatedContentHandler contentHandler )
    {
        throw new UnsupportedOperationException( "No handler for this " + this.getClass() );
    }
}
