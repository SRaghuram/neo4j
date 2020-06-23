/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.util.Id;

import static java.lang.String.format;

public class MemberId
{
    private final Id id;

    public MemberId( UUID uuid )
    {
        id = new Id( uuid );
    }

    public UUID getUuid()
    {
        return id.uuid();
    }

    @Override
    public String toString()
    {
        return format( "MemberId{%s}", id );
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

        MemberId that = (MemberId) o;
        return Objects.equals( id, that.id );
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( id );
    }

    /**
     * Format:
     * ┌──────────────────────────────┐
     * │mostSignificantBits    8 bytes│
     * │leastSignificantBits   8 bytes│
     * └──────────────────────────────┘
     */
    public static class Marshal extends SafeStateMarshal<MemberId>
    {
        public static final Marshal INSTANCE = new Marshal();

        @Override
        public void marshal( MemberId memberId, WritableChannel channel ) throws IOException
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
        public MemberId unmarshal0( ReadableChannel channel ) throws IOException
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
                return new MemberId( new UUID( mostSigBits, leastSigBits ) );
            }
        }

        @Override
        public MemberId startState()
        {
            return null;
        }

        @Override
        public long ordinal( MemberId memberId )
        {
            return memberId == null ? 0 : 1;
        }
    }
}
