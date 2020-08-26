/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.util.Id;

public final class RaftMemberId
{
    private final Id id;

    RaftMemberId( UUID uuid )
    {
        this.id = new Id( uuid );
    }

    public static RaftMemberId of( UUID id )
    {
        return new RaftMemberId( id );
    }

    /**
     * This conversion should be only temporary, until mapping is implemented in Topology
     */
    @Deprecated
    public static RaftMemberId from( ServerId memberId )
    {
        return new RaftMemberId( memberId.getUuid() );
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
        RaftMemberId raftId = (RaftMemberId) o;
        return Objects.equals( id, raftId.id );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( id );
    }

    public UUID getUuid()
    {
        return id.uuid();
    }

    @Override
    public String toString()
    {
        return "RaftMemberId{" + id + '}';
    }

    /**
     * Format:
     * ┌──────────────────────────────┐
     * │mostSignificantBits    8 bytes│
     * │leastSignificantBits   8 bytes│
     * └──────────────────────────────┘
     */
    public static class Marshal extends SafeStateMarshal<RaftMemberId>
    {
        public static final RaftMemberId.Marshal INSTANCE = new RaftMemberId.Marshal();

        @Override
        public void marshal( RaftMemberId memberId, WritableChannel channel ) throws IOException
        {
            if ( memberId == null )
            {
                channel.put( (byte) 0 );
            }
            else
            {
                channel.put( (byte) 1 );
                channel.putLong( memberId.getUuid().getMostSignificantBits() );
                channel.putLong( memberId.getUuid().getLeastSignificantBits() );
            }
        }

        @Override
        public RaftMemberId unmarshal0( ReadableChannel channel ) throws IOException
        {
            byte nullMarker = channel.get();
            if ( nullMarker == 0 )
            {
                return null;
            }
            else
            {
                long mostSigBits = channel.getLong();
                long leastSigBits = channel.getLong();
                return new RaftMemberId( new UUID( mostSigBits, leastSigBits ) );
            }
        }

        @Override
        public RaftMemberId startState()
        {
            return null;
        }

        @Override
        public long ordinal( RaftMemberId memberId )
        {
            return memberId == null ? 0 : 1;
        }
    }
}
