/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.messaging.EndOfStreamException;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public class UUIDMarshal extends SafeChannelMarshal<UUID>
{
    public static final UUIDMarshal INSTANCE = new UUIDMarshal();

    @Override
    protected UUID unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var msb = channel.getLong();
        var lsb = channel.getLong();
        return new UUID( msb, lsb );
    }

    @Override
    public void marshal( UUID uuid, WritableChannel channel ) throws IOException
    {
        channel.putLong( uuid.getMostSignificantBits() );
        channel.putLong( uuid.getLeastSignificantBits() );
    }
}
