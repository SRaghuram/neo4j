/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.core.state.CoreStateFiles;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

import static java.lang.String.format;

public class CoreSnapshot
{
    private final long prevIndex;
    private final long prevTerm;

    private final Map<CoreStateFiles<?>,Object> snapshotCollection = new HashMap<>();

    public CoreSnapshot( long prevIndex, long prevTerm )
    {
        this.prevIndex = prevIndex;
        this.prevTerm = prevTerm;
    }

    public long prevIndex()
    {
        return prevIndex;
    }

    public long prevTerm()
    {
        return prevTerm;
    }

    public <T> void add( CoreStateFiles<T> type, T state )
    {
        snapshotCollection.put( type, state );
    }

    public <T> T get( CoreStateFiles<T> type )
    {
        return (T) snapshotCollection.get( type );
    }

    public int size()
    {
        return snapshotCollection.size();
    }

    public static class Marshal extends SafeChannelMarshal<CoreSnapshot>
    {
        @Override
        public void marshal( CoreSnapshot coreSnapshot, WritableChannel channel ) throws IOException
        {
            channel.putLong( coreSnapshot.prevIndex );
            channel.putLong( coreSnapshot.prevTerm );

            channel.putInt( coreSnapshot.size() );
            for ( Map.Entry<CoreStateFiles<?>,Object> entry : coreSnapshot.snapshotCollection.entrySet() )
            {
                CoreStateFiles type = entry.getKey();
                Object state = entry.getValue();

                channel.putInt( type.typeId() );
                type.marshal().marshal( state, channel );
            }
        }

        @Override
        public CoreSnapshot unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long prevIndex = channel.getLong();
            long prevTerm = channel.getLong();

            CoreSnapshot coreSnapshot = new CoreSnapshot( prevIndex, prevTerm );
            int snapshotCount = channel.getInt();
            for ( int i = 0; i < snapshotCount; i++ )
            {
                int typeOrdinal = channel.getInt();
                CoreStateFiles type = CoreStateFiles.values().get( typeOrdinal );
                Object state = type.marshal().unmarshal( channel );
                coreSnapshot.add( type, state );
            }

            return coreSnapshot;
        }
    }

    @Override
    public String toString()
    {
        return format( "CoreSnapshot{prevIndex=%d, prevTerm=%d, snapshotCollection=%s}", prevIndex, prevTerm, snapshotCollection );
    }
}
