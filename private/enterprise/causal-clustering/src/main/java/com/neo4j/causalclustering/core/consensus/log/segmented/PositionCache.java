/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.LogPosition;

import static java.util.Arrays.fill;

/**
 * Caches (offsetIndex) -> (byteOffset) mappings, which can be used to find an exact or
 * approximate byte position for an entry given an index. The index is defined as a relative
 * offset index starting from 0 for each segment, instead of the absolute logIndex in the
 * log file.
 *
 * The necessity and efficiency of this cache is understood by considering the values put into
 * it. When closing cursors the position after the last entry is cached so that when the next
 * batch of entries are to be read the position is already known.
 */
class PositionCache
{
    static final int CACHE_SIZE = 8;

    private final LogPosition first;

    private final LogPosition[] cache = new LogPosition[CACHE_SIZE];
    private int pos;

    PositionCache( long recordOffset )
    {
        first = new LogPosition( 0, recordOffset );
        fill( cache, first );
    }

    /**
     * Saves a known position in the cache.
     *
     * @param position The position which should interpreted as (offsetIndex, byteOffset).
     */
    public synchronized void put( LogPosition position )
    {
        cache[pos] = position;
        pos = (pos + 1) % CACHE_SIZE;
    }

    /**
     * Returns a position at or before the searched offsetIndex, the closest known.
     * Users will have to scan forward to reach the exact position.
     *
     * @param offsetIndex The relative index.
     * @return A position at or before the searched offsetIndex.
     */
    synchronized LogPosition lookup( long offsetIndex )
    {
        if ( offsetIndex == 0 )
        {
            return first;
        }

        LogPosition best = first;

        for ( int i = 0; i < CACHE_SIZE; i++ )
        {
            if ( cache[i].logIndex <= offsetIndex && cache[i].logIndex > best.logIndex )
            {
                best = cache[i];
            }
        }

        return best;
    }
}
