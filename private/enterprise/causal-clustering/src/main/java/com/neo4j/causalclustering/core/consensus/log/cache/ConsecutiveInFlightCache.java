/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;

/**
 * A cache that keeps Raft log entries in memory, generally to bridge the gap
 * between the time that a Raft log is being appended to the local Raft log and
 * at a later time applied to the store. This cache optimises for the up-to-date
 * case and does not cater specifically for lagging followers. It is better to let
 * those catch up from entries read from disk where possible.
 * <p>
 * The cache relies on highly efficient underlying data structures (a circular
 * buffer) and also allows on to specify a maximum bound on the number of entries
 * as well as their total size where known, see {@link ReplicatedContent#size()} ()}.
 */
public class ConsecutiveInFlightCache implements InFlightCache
{
    private final ConsecutiveCache<RaftLogEntry> cache;
    private final RaftLogEntry[] evictions;
    private final InFlightCacheMonitor monitor;

    private long totalBytes;
    private long maxBytes;
    private boolean enabled;

    public ConsecutiveInFlightCache()
    {
        this( 1024, 8 * 1024 * 1024, InFlightCacheMonitor.VOID, true );
    }

    public ConsecutiveInFlightCache( int capacity, long maxBytes, InFlightCacheMonitor monitor, boolean enabled )
    {
        this.cache = new ConsecutiveCache<>( capacity );
        this.evictions = new RaftLogEntry[capacity];

        this.maxBytes = maxBytes;
        this.monitor = monitor;
        this.enabled = enabled;

        monitor.setMaxBytes( maxBytes );
        monitor.setMaxElements( capacity );
    }

    @Override
    public synchronized void enable()
    {
        enabled = true;
    }

    @Override
    public synchronized void put( long logIndex, RaftLogEntry entry )
    {
        if ( !enabled )
        {
            return;
        }

        totalBytes += sizeOf( entry );
        cache.put( logIndex, entry, evictions );
        processEvictions();

        while ( totalBytes > maxBytes )
        {
            RaftLogEntry evicted = cache.remove();
            totalBytes -= sizeOf( evicted );
        }
    }

    @Override
    public synchronized RaftLogEntry get( long logIndex )
    {
        if ( !enabled )
        {
            monitor.miss();
            return null;
        }

        RaftLogEntry entry = cache.get( logIndex );

        if ( entry == null )
        {
            monitor.miss();
        }
        else
        {
            monitor.hit();
        }

        return entry;
    }

    @Override
    public synchronized void truncate( long fromIndex )
    {
        if ( !enabled )
        {
            return;
        }

        cache.truncate( fromIndex, evictions );
        processEvictions();
    }

    @Override
    public synchronized void prune( long upToIndex )
    {
        if ( !enabled )
        {
            return;
        }

        cache.prune( upToIndex, evictions );
        processEvictions();
    }

    @Override
    public synchronized long totalBytes()
    {
        return totalBytes;
    }

    @Override
    public synchronized int elementCount()
    {
        return cache.size();
    }

    @Override
    public void reportSkippedCacheAccess()
    {
        monitor.miss();
    }

    private long sizeOf( RaftLogEntry entry )
    {
        return entry.content().size().orElse( 0L );
    }

    private void processEvictions()
    {
        for ( int i = 0; i < evictions.length; i++ )
        {
            RaftLogEntry entry = evictions[i];
            if ( entry == null )
            {
                break;
            }
            evictions[i] = null;
            totalBytes -= sizeOf( entry );
        }

        monitor.setTotalBytes( totalBytes );
        monitor.setElementCount( cache.size() );
    }
}
