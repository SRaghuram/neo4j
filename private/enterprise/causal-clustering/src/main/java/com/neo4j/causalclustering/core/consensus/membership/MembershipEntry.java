/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;

/**
 * Represents a membership entry in the RAFT log.
 */
public class MembershipEntry
{
    private long logIndex;
    private Set<RaftMemberId> members;

    public MembershipEntry( long logIndex, Set<RaftMemberId> members )
    {
        this.members = members;
        this.logIndex = logIndex;
    }

    public long logIndex()
    {
        return logIndex;
    }

    public Set<RaftMemberId> members()
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
        MembershipEntry that = (MembershipEntry) o;
        return logIndex == that.logIndex && Objects.equals( members, that.members );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( logIndex, members );
    }

    @Override
    public String toString()
    {
        return "MembershipEntry{" +
               "logIndex=" + logIndex +
               ", members=" + members +
               '}';
    }

    public static class Marshal extends SafeStateMarshal<MembershipEntry>
    {
        RaftMemberId.Marshal memberMarshal = new RaftMemberId.Marshal();

        @Override
        public MembershipEntry startState()
        {
            return null;
        }

        @Override
        public long ordinal( MembershipEntry entry )
        {
            return entry.logIndex;
        }

        @Override
        public void marshal( MembershipEntry entry, WritableChannel channel ) throws IOException
        {
            if ( entry == null )
            {
                channel.putInt( 0 );
                return;
            }
            else
            {
                channel.putInt( 1 );
            }

            channel.putLong( entry.logIndex );
            channel.putInt( entry.members.size() );
            for ( RaftMemberId member : entry.members )
            {
                memberMarshal.marshal( member, channel );
            }
        }

        @Override
        protected MembershipEntry unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            int hasEntry = channel.getInt();
            if ( hasEntry == 0 )
            {
                return null;
            }
            long logIndex = channel.getLong();
            int memberCount = channel.getInt();
            Set<RaftMemberId> members = new HashSet<>();
            for ( int i = 0; i < memberCount; i++ )
            {
                members.add( memberMarshal.unmarshal( channel ) );
            }
            return new MembershipEntry( logIndex, members );
        }
    }
}
