/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static java.lang.String.format;

public class RaftTestMembers implements RaftMembers<RaftMemberId>
{
    private final Set<RaftMemberId> members = new HashSet<>();

    public RaftTestMembers( Set<RaftMemberId> members )
    {
        this.members.addAll( members );
    }

    public RaftTestMembers( int... memberIds )
    {
        for ( int memberId : memberIds )
        {
            this.members.add( raftMember( memberId ) );
        }
    }

    public RaftTestMembers( RaftMemberId... memberIds )
    {
        this.members.addAll( Arrays.asList( memberIds ) );
    }

    @Override
    public Set<RaftMemberId> getMembers()
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

        RaftTestMembers that = (RaftTestMembers) o;

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
