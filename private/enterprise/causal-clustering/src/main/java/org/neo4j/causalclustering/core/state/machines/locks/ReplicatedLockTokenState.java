/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest.INVALID_REPLICATED_LOCK_TOKEN_REQUEST;

public class ReplicatedLockTokenState
{
    private static long INITIAL_ORDINAL = -1;
    private long ordinal;
    private MemberId owner;
    private int candidateId;

    public ReplicatedLockTokenState()
    {
        this( INITIAL_ORDINAL, INVALID_REPLICATED_LOCK_TOKEN_REQUEST );
    }

    public ReplicatedLockTokenState( long ordinal, ReplicatedLockTokenRequest currentToken )
    {
        this( ordinal, currentToken.id(), currentToken.owner() );
    }

    private ReplicatedLockTokenState( long ordinal, int candidateId, MemberId owner )
    {
       this.ordinal = ordinal;
       this.candidateId = candidateId;
       this.owner = owner;
    }

    public void set( ReplicatedLockTokenRequest currentToken, long ordinal )
    {
        candidateId = currentToken.id();
        owner = currentToken.owner();
        this.ordinal = ordinal;
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
        return String.format( "ReplicatedLockTokenState{candidateId=%s, owner=%s, ordinal=%d}", candidateId, owner, ordinal );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof ReplicatedLockTokenState) )
        {
            return false;
        }
        ReplicatedLockTokenState that = (ReplicatedLockTokenState) o;
        return ordinal == that.ordinal && candidateId == that.candidateId && Objects.equals( owner, that.owner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( ordinal, owner, candidateId );
    }

    ReplicatedLockTokenState newInstance()
    {
        return new ReplicatedLockTokenState( ordinal, candidateId, owner );
    }

    public static class Marshal extends SafeStateMarshal<ReplicatedLockTokenState>
    {
        private final ChannelMarshal<MemberId> memberMarshal;

        public Marshal()
        {
            this.memberMarshal = MemberId.Marshal.INSTANCE;
        }

        @Override
        public void marshal( ReplicatedLockTokenState state,
                             WritableChannel channel ) throws IOException
        {
            channel.putLong( state.ordinal );
            channel.putInt( state.candidateId() );
            memberMarshal.marshal( state.owner(), channel );
        }

        @Override
        public ReplicatedLockTokenState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long logIndex = channel.getLong();
            int candidateId = channel.getInt();
            MemberId member = memberMarshal.unmarshal( channel );
            return new ReplicatedLockTokenState( logIndex, candidateId, member );
        }

        @Override
        public ReplicatedLockTokenState startState()
        {
            return new ReplicatedLockTokenState();
        }

        @Override
        public long ordinal( ReplicatedLockTokenState state )
        {
            return state.ordinal();
        }
    }
}
