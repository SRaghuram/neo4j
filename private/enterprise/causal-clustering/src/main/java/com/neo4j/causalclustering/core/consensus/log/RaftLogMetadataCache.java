/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;


import java.util.Objects;
import java.util.function.LongPredicate;

import org.neo4j.internal.helpers.collection.LruCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

public class RaftLogMetadataCache
{
    private final LruCache<Long /*tx id*/, RaftLogEntryMetadata> raftLogEntryCache;

    public RaftLogMetadataCache( int logEntryCacheSize )
    {
        this.raftLogEntryCache = new LruCache<>( "Raft log entry cache", logEntryCacheSize );
    }

    public void clear()
    {
        raftLogEntryCache.clear();
    }

    /**
     * Returns the metadata for the entry at position {@param logIndex}, null if the metadata is not present in the cache
     */
    public RaftLogEntryMetadata getMetadata( long logIndex )
    {
        return raftLogEntryCache.get( logIndex );
    }

    public RaftLogEntryMetadata cacheMetadata( long logIndex, long entryTerm, LogPosition position )
    {
        RaftLogEntryMetadata result = new RaftLogEntryMetadata( entryTerm, position );
        raftLogEntryCache.put( logIndex, result );
        return result;
    }

    public void removeUpTo( long upTo )
    {
        remove( key -> key <= upTo );
    }

    public void removeUpwardsFrom( long startingFrom )
    {
        remove( key -> key >= startingFrom );
    }

    private void remove( LongPredicate predicate )
    {
        raftLogEntryCache.keySet().removeIf( predicate::test );
    }

    public static class RaftLogEntryMetadata
    {
        private final long entryTerm;
        private final LogPosition startPosition;

        public RaftLogEntryMetadata( long entryTerm, LogPosition startPosition )
        {
            Objects.requireNonNull( startPosition );
            this.entryTerm = entryTerm;
            this.startPosition = startPosition;
        }

        public long getEntryTerm()
        {
            return entryTerm;
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

            RaftLogEntryMetadata that = (RaftLogEntryMetadata) o;

            if ( entryTerm != that.entryTerm )
            {
                return false;
            }
            return startPosition.equals( that.startPosition );

        }

        @Override
        public int hashCode()
        {
            int result = (int) (entryTerm ^ (entryTerm >>> 32));
            result = 31 * result + startPosition.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return "RaftLogEntryMetadata{" +
                    "entryTerm=" + entryTerm +
                    ", startPosition=" + startPosition +
                    '}';
        }
    }
}
