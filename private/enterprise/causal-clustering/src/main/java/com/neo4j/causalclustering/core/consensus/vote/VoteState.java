/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.vote;

import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeStateMarshal;

public class VoteState
{
    private RaftMemberId votedFor;
    private long term = -1;

    public VoteState()
    {
    }

    private VoteState( RaftMemberId votedFor, long term )
    {
        this.term = term;
        this.votedFor = votedFor;
    }

    public RaftMemberId votedFor()
    {
        return votedFor;
    }

    public boolean update( RaftMemberId votedFor, long term )
    {
        if ( termChanged( term ) )
        {
            this.votedFor = votedFor;
            this.term = term;
            return true;
        }
        else
        {
            if ( this.votedFor == null )
            {
                if ( votedFor != null )
                {
                    this.votedFor = votedFor;
                    return true;
                }
            }
            else if ( !this.votedFor.equals( votedFor ) )
            {
                throw new IllegalArgumentException( "Can only vote once per term." );
            }
            return false;
        }
    }

    private boolean termChanged( long term )
    {
        return term != this.term;
    }

    public long term()
    {
        return term;
    }

    public static class Marshal extends SafeStateMarshal<VoteState>
    {
        private final ChannelMarshal<RaftMemberId> memberMarshal;

        public Marshal()
        {
            this.memberMarshal = RaftMemberId.Marshal.INSTANCE;
        }

        @Override
        public void marshal( VoteState state, WritableChannel channel ) throws IOException
        {
            channel.putLong( state.term );
            memberMarshal.marshal( state.votedFor(), channel );
        }

        @Override
        public VoteState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            final long term = channel.getLong();
            final RaftMemberId member = memberMarshal.unmarshal( channel );
            return new VoteState( member, term );
        }

        @Override
        public VoteState startState()
        {
            return new VoteState();
        }

        @Override
        public long ordinal( VoteState state )
        {
            return state.term();
        }
    }

    @Override
    public String toString()
    {
        return "VoteState{" +
               "votedFor=" + votedFor +
               ", term=" + term +
               '}';
    }
}
