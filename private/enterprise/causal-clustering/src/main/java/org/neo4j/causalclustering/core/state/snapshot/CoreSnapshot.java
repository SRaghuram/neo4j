/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.neo4j.causalclustering.core.state.CoreStateFiles;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.String.format;

public class CoreSnapshot
{
    private final long prevIndex;
    private final long prevTerm;

    //Casts throughout this class are safe because we check that the state type corresponds to the CoreStateFiles type parameter on insertion
    private final Map<Pair<String,CoreStateFiles>,Object> snapshotCollection = new HashMap<>();

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

    public <T> void add( String databaseName, CoreStateFiles<T> type, T state )
    {
        snapshotCollection.put( Pair.of( databaseName, type ), state );
    }

    public <T> void add( CoreStateFiles<T> type, T state )
    {
        add( null, type, state );
    }

    public <T> T get( String databaseName, CoreStateFiles<T> type )
    {
        //noinspection unchecked
        return (T) snapshotCollection.get( Pair.of( databaseName, type) );
    }

    public <T> T get( CoreStateFiles<T> type )
    {
        return get( null, type );
    }

    public <T> void forEachSnapshotOfType( CoreStateFiles<T> type, BiConsumer<String, T> consumer )
    {
        //noinspection unchecked
        snapshotCollection.entrySet().stream()
                .filter( entry -> entry.getKey().other().equals( type ) )
                .forEach( entry -> consumer.accept( entry.getKey().first(), (T) entry.getValue() ) );
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
            for ( Map.Entry<Pair<String,CoreStateFiles>,Object> entry : coreSnapshot.snapshotCollection.entrySet() )
            {
                CoreStateFiles type = entry.getKey().other();
                channel.putInt( type.typeId() );
                //noinspection unchecked
                type.marshal().marshal( entry.getValue(), channel );
                StringMarshal.marshal( channel, entry.getKey().first() );
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
                String databaseName = StringMarshal.unmarshal( channel );
                //noinspection unchecked
                coreSnapshot.add( databaseName, type, state );
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
