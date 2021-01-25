/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.SafeStateMarshal;
import org.neo4j.util.Id;

public final class RaftMemberId extends Id
{
    public RaftMemberId( UUID uuid )
    {
        super( uuid );
    }

    @Override
    public String toString()
    {
        return "RaftMemberId{" + shortName() + '}';
    }

    public static class Marshal extends SafeStateMarshal<RaftMemberId>
    {
        public static final RaftMemberId.Marshal INSTANCE = new RaftMemberId.Marshal();

        private Marshal()
        {
            // use INSTANCE
        }

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
                channel.putLong( memberId.uuid().getMostSignificantBits() );
                channel.putLong( memberId.uuid().getLeastSignificantBits() );
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
