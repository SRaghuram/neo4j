/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public class RaftId
{
    private final UUID uuid;

    public RaftId( UUID uuid )
    {
        this.uuid = uuid;
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
        RaftId raftId = (RaftId) o;
        return Objects.equals( uuid, raftId.uuid );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( uuid );
    }

    public UUID uuid()
    {
        return uuid;
    }

    @Override
    public String toString()
    {
        return "RaftId{" +
               "uuid=" + uuid +
               '}';
    }

    public static class Marshal extends SafeStateMarshal<RaftId>
    {
        public static final Marshal INSTANCE = new Marshal();
        private static final UUID NIL = new UUID( 0L, 0L );

        @Override
        public void marshal( RaftId raftId, WritableChannel channel ) throws IOException
        {
            UUID uuid = raftId == null ? NIL : raftId.uuid;
            channel.putLong( uuid.getMostSignificantBits() );
            channel.putLong( uuid.getLeastSignificantBits() );
        }

        @Override
        public RaftId unmarshal0( ReadableChannel channel ) throws IOException
        {
            long mostSigBits = channel.getLong();
            long leastSigBits = channel.getLong();
            UUID uuid = new UUID( mostSigBits, leastSigBits );

            return uuid.equals( NIL ) ? null : new RaftId( uuid );
        }

        @Override
        public RaftId startState()
        {
            return null;
        }

        @Override
        public long ordinal( RaftId raftId )
        {
            return raftId == null ? 0 : 1;
        }
    }
}
