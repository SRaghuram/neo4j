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
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.util.Id;

public final class RaftGroupId extends Id
{
    public RaftGroupId( UUID uuid )
    {
        super( uuid );
    }

    public static RaftGroupId from( DatabaseId databaseId )
    {
        return new RaftGroupId( databaseId.uuid() );
    }

    @Override
    public String toString()
    {
        return "RaftGroupId{" + shortName() + '}';
    }

    public static class Marshal extends SafeStateMarshal<RaftGroupId>
    {
        public static final Marshal INSTANCE = new Marshal();
        private static final UUID NIL = new UUID( 0L, 0L );

        private Marshal()
        {
            // use INSTANCE
        }

        @Override
        public void marshal( RaftGroupId raftGroupId, WritableChannel channel ) throws IOException
        {
            UUID uuid = raftGroupId == null ? NIL : raftGroupId.uuid();
            channel.putLong( uuid.getMostSignificantBits() );
            channel.putLong( uuid.getLeastSignificantBits() );
        }

        @Override
        public RaftGroupId unmarshal0( ReadableChannel channel ) throws IOException
        {
            long mostSigBits = channel.getLong();
            long leastSigBits = channel.getLong();
            UUID uuid = new UUID( mostSigBits, leastSigBits );

            return uuid.equals( NIL ) ? null : new RaftGroupId( uuid );
        }

        @Override
        public RaftGroupId startState()
        {
            return null;
        }

        @Override
        public long ordinal( RaftGroupId raftGroupId )
        {
            return raftGroupId == null ? 0 : 1;
        }
    }
}
