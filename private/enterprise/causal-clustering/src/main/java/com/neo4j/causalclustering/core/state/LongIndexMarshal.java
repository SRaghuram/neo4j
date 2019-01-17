/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;

import java.io.IOException;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * A marshal for an index that starts with -1 at the empty slot before the first real entry at 0.
 */
public class LongIndexMarshal extends SafeStateMarshal<Long>
{
    @Override
    public Long startState()
    {
        return -1L;
    }

    @Override
    public long ordinal( Long index )
    {
        return index;
    }

    @Override
    public void marshal( Long index, WritableChannel channel ) throws IOException
    {
        channel.putLong( index );
    }

    @Override
    protected Long unmarshal0( ReadableChannel channel ) throws IOException
    {
        return channel.getLong();
    }
}
