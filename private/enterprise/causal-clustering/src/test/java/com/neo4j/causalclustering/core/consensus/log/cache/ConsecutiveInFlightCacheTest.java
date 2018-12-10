/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConsecutiveInFlightCacheTest
{
    @Test
    public void shouldTrackUsedMemory()
    {
        int capacity = 4;
        ConsecutiveInFlightCache cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        for ( int i = 0; i < capacity; i++ )
        {
            // when
            cache.put( i, content( 100 ) );

            // then
            assertEquals( (i + 1) * 100, cache.totalBytes() );
        }

        // when
        cache.put( capacity, content( 100 ) );

        // then
        assertEquals( capacity, cache.elementCount() );
        assertEquals( capacity * 100, cache.totalBytes() );

        // when
        cache.put( capacity + 1, content( 500 ) );
        assertEquals( capacity, cache.elementCount() );
        assertEquals( 800, cache.totalBytes() );

        // when
        cache.put( capacity + 2, content( 500 ) );
        assertEquals( 2, cache.elementCount() );
        assertEquals( 1000, cache.totalBytes() );
    }

    @Test
    public void shouldReturnLatestItems()
    {
        // given
        int capacity = 4;
        ConsecutiveInFlightCache cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        // when
        for ( int i = 0; i < 3 * capacity; i++ )
        {
            cache.put( i, content( i ) );
        }

        // then
        for ( int i = 0; i < 3 * capacity; i++ )
        {
            RaftLogEntry entry = cache.get( i );
            if ( i < 2 * capacity )
            {
                assertNull( entry );
            }
            else
            {
                assertTrue( entry.content().size().isPresent() );
                assertEquals( i, entry.content().size().getAsLong() );
            }
        }
    }

    @Test
    public void shouldRemovePrunedItems()
    {
        // given
        int capacity = 20;
        ConsecutiveInFlightCache cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, content( i ) );
        }

        // when
        int upToIndex = capacity / 2 - 1;
        cache.prune( upToIndex );

        // then
        assertEquals( capacity / 2, cache.elementCount() );

        for ( int i = 0; i < capacity; i++ )
        {
            RaftLogEntry entry = cache.get( i );
            if ( i <= upToIndex )
            {
                assertNull( entry );
            }
            else
            {
                assertTrue( entry.content().size().isPresent() );
                assertEquals( i, entry.content().size().getAsLong() );
            }
        }
    }

    @Test
    public void shouldRemoveTruncatedItems()
    {
        // given
        int capacity = 20;
        ConsecutiveInFlightCache cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, content( i ) );
        }

        // when
        int fromIndex = capacity / 2;
        cache.truncate( fromIndex );

        // then
        assertEquals( fromIndex, cache.elementCount() );
        assertEquals( (fromIndex * (fromIndex - 1)) / 2, cache.totalBytes() );

        for ( int i = fromIndex; i < capacity; i++ )
        {
            assertNull( cache.get( i ) );
        }
    }

    private RaftLogEntry content( int size )
    {
        return new RaftLogEntry( 0, new DummyRequest( new byte[size] ) );
    }
}
