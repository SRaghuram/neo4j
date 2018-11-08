/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.identity;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ClusterId
{
    private final UUID uuid;

    public ClusterId( UUID uuid )
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
        ClusterId clusterId = (ClusterId) o;
        return Objects.equals( uuid, clusterId.uuid );
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
        return "ClusterId{" +
               "uuid=" + uuid +
               '}';
    }

    public static class Marshal extends SafeStateMarshal<ClusterId>
    {
        public static final Marshal INSTANCE = new Marshal();
        private static final UUID NIL = new UUID( 0L, 0L );

        @Override
        public void marshal( ClusterId clusterId, WritableChannel channel ) throws IOException
        {
            UUID uuid = clusterId == null ? NIL : clusterId.uuid;
            channel.putLong( uuid.getMostSignificantBits() );
            channel.putLong( uuid.getLeastSignificantBits() );
        }

        @Override
        public ClusterId unmarshal0( ReadableChannel channel ) throws IOException
        {
            long mostSigBits = channel.getLong();
            long leastSigBits = channel.getLong();
            UUID uuid = new UUID( mostSigBits, leastSigBits );

            return uuid.equals( NIL ) ? null : new ClusterId( uuid );
        }

        @Override
        public ClusterId startState()
        {
            return null;
        }

        @Override
        public long ordinal( ClusterId clusterId )
        {
            return clusterId == null ? 0 : 1;
        }
    }
}
