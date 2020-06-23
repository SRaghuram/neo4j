/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;

import static com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseRequest.INVALID_LEASE_REQUEST;

public class ReplicatedLeaseState
{
    public static final ReplicatedLeaseState INITIAL_LEASE_STATE = new ReplicatedLeaseState( -1, INVALID_LEASE_REQUEST );

    private final long ordinal;
    private final MemberId owner;
    private final int leaseId;

    public ReplicatedLeaseState( long ordinal, ReplicatedLeaseRequest leaseRequest )
    {
        this( ordinal, leaseRequest.id(), leaseRequest.owner() );
    }

    private ReplicatedLeaseState( long ordinal, int leaseId, MemberId owner )
    {
       this.ordinal = ordinal;
       this.leaseId = leaseId;
       this.owner = owner;
    }

    public int leaseId()
    {
        return leaseId;
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
        return String.format( "ReplicatedLeaseState{leaseId=%s, owner=%s, ordinal=%d}", leaseId, owner, ordinal );
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
        ReplicatedLeaseState that = (ReplicatedLeaseState) o;
        return ordinal == that.ordinal && leaseId == that.leaseId && Objects.equals( owner, that.owner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( ordinal, owner, leaseId );
    }

    ReplicatedLeaseState newInstance()
    {
        return new ReplicatedLeaseState( ordinal, leaseId, owner );
    }

    public static class Marshal extends SafeStateMarshal<ReplicatedLeaseState>
    {
        private final ChannelMarshal<MemberId> memberMarshal;

        public Marshal()
        {
            this.memberMarshal = MemberId.Marshal.INSTANCE;
        }

        @Override
        public void marshal( ReplicatedLeaseState state,
                             WritableChannel channel ) throws IOException
        {
            channel.putLong( state.ordinal );
            channel.putInt( state.leaseId() );
            memberMarshal.marshal( state.owner(), channel );
        }

        @Override
        public ReplicatedLeaseState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long logIndex = channel.getLong();
            int leaseId = channel.getInt();
            MemberId member = memberMarshal.unmarshal( channel );
            return new ReplicatedLeaseState( logIndex, leaseId, member );
        }

        @Override
        public ReplicatedLeaseState startState()
        {
            return INITIAL_LEASE_STATE;
        }

        @Override
        public long ordinal( ReplicatedLeaseState state )
        {
            return state.ordinal();
        }
    }
}

