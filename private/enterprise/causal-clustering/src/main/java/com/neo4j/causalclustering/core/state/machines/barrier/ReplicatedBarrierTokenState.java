/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import java.io.IOException;
import java.util.Objects;

import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

import static com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenRequest.INVALID_REPLICATED_BARRIER_TOKEN_REQUEST;

public class ReplicatedBarrierTokenState
{
    public static final ReplicatedBarrierTokenState INITIAL_BARRIER_TOKEN =
            new ReplicatedBarrierTokenState( -1, INVALID_REPLICATED_BARRIER_TOKEN_REQUEST );

    private final long ordinal;
    private final MemberId owner;
    private final int candidateId;

    public ReplicatedBarrierTokenState( long ordinal, ReplicatedBarrierTokenRequest currentToken )
    {
        this( ordinal, currentToken.id(), currentToken.owner() );
    }

    private ReplicatedBarrierTokenState( long ordinal, int candidateId, MemberId owner )
    {
       this.ordinal = ordinal;
       this.candidateId = candidateId;
       this.owner = owner;
    }

    public int candidateId()
    {
        return candidateId;
    }

    public MemberId owner()
    {
        return owner;
    }

    long ordinal()
    {
        return ordinal;
    }

    @Override
    public String toString()
    {
        return String.format( "ReplicatedBarrierTokenState{candidateId=%s, owner=%s, ordinal=%d}", candidateId, owner, ordinal );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof ReplicatedBarrierTokenState) )
        {
            return false;
        }
        ReplicatedBarrierTokenState that = (ReplicatedBarrierTokenState) o;
        return ordinal == that.ordinal && candidateId == that.candidateId && Objects.equals( owner, that.owner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( ordinal, owner, candidateId );
    }

    ReplicatedBarrierTokenState newInstance()
    {
        return new ReplicatedBarrierTokenState( ordinal, candidateId, owner );
    }

    public static class Marshal extends SafeStateMarshal<ReplicatedBarrierTokenState>
    {
        private final ChannelMarshal<MemberId> memberMarshal;

        public Marshal()
        {
            this.memberMarshal = MemberId.Marshal.INSTANCE;
        }

        @Override
        public void marshal( ReplicatedBarrierTokenState state,
                             WritableChannel channel ) throws IOException
        {
            channel.putLong( state.ordinal );
            channel.putInt( state.candidateId() );
            memberMarshal.marshal( state.owner(), channel );
        }

        @Override
        public ReplicatedBarrierTokenState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long logIndex = channel.getLong();
            int candidateId = channel.getInt();
            MemberId member = memberMarshal.unmarshal( channel );
            return new ReplicatedBarrierTokenState( logIndex, candidateId, member );
        }

        @Override
        public ReplicatedBarrierTokenState startState()
        {
            return INITIAL_BARRIER_TOKEN;
        }

        @Override
        public long ordinal( ReplicatedBarrierTokenState state )
        {
            return state.ordinal();
        }
    }
}

