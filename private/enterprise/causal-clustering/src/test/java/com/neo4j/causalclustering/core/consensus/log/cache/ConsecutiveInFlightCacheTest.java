/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ConsecutiveInFlightCacheTest
{
    @Test
    void shouldTrackUsedMemory()
    {
        var capacity = 4;
        var cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        for ( var i = 0; i < capacity; i++ )
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
    void shouldReturnLatestItems()
    {
        // given
        var capacity = 4;
        var cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        // when
        for ( var i = 0; i < 3 * capacity; i++ )
        {
            cache.put( i, content( i ) );
        }

        // then
        for ( var i = 0; i < 3 * capacity; i++ )
        {
            var entry = cache.get( i );
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
    void shouldRemovePrunedItems()
    {
        // given
        var capacity = 20;
        var cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        for ( var i = 0; i < capacity; i++ )
        {
            cache.put( i, content( i ) );
        }

        // when
        var upToIndex = capacity / 2 - 1;
        cache.prune( upToIndex );

        // then
        assertEquals( capacity / 2, cache.elementCount() );

        for ( var i = 0; i < capacity; i++ )
        {
            var entry = cache.get( i );
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
    void shouldRemoveTruncatedItems()
    {
        // given
        var capacity = 20;
        var cache = new ConsecutiveInFlightCache( capacity, 1000, InFlightCacheMonitor.VOID, true );

        for ( var i = 0; i < capacity; i++ )
        {
            cache.put( i, content( i ) );
        }

        // when
        var fromIndex = capacity / 2;
        cache.truncate( fromIndex );

        // then
        assertEquals( fromIndex, cache.elementCount() );
        assertEquals( (fromIndex * (fromIndex - 1)) / 2, cache.totalBytes() );

        for ( var i = fromIndex; i < capacity; i++ )
        {
            assertNull( cache.get( i ) );
        }
    }

    @Test
    void shouldReportCacheMissWhenCacheAccessSkipped()
    {
        var cacheMonitor = mock( InFlightCacheMonitor.class );
        var cache = new ConsecutiveInFlightCache( 1, 1, cacheMonitor, true );

        cache.reportSkippedCacheAccess();

        verify( cacheMonitor ).miss();
    }

    @Test
    void shouldReportCacheMissWhenDisabled()
    {
        var cacheMonitor = mock( InFlightCacheMonitor.class );
        var cache = new ConsecutiveInFlightCache( 1, 1, cacheMonitor, false );

        assertNull( cache.get( 1 ) );

        verify( cacheMonitor ).miss();
    }

    private static RaftLogEntry content( int size )
    {
        return new RaftLogEntry( 0, new DummyRequest( new byte[size] ) );
    }
}
