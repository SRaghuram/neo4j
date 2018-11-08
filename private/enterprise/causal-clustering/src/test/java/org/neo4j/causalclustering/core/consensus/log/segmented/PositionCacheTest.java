/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.log.LogPosition;

import static org.junit.Assert.assertEquals;

public class PositionCacheTest
{
    private final long recordOffset = SegmentHeader.CURRENT_RECORD_OFFSET;
    private final PositionCache cache = new PositionCache( recordOffset );
    private final LogPosition FIRST = new LogPosition( 0, recordOffset );

    @Test
    public void shouldReturnSaneDefaultPosition()
    {
        // when
        LogPosition position = cache.lookup( 5 );

        // then
        assertEquals( FIRST, position );
    }

    @Test
    public void shouldReturnBestPosition()
    {
        // given
        cache.put( pos( 4 ) );
        cache.put( pos( 6 ) );

        // when
        LogPosition lookup = cache.lookup( 7 );

        // then
        assertEquals( pos( 6 ), lookup );
    }

    @Test
    public void shouldReturnExactMatch()
    {
        // given
        cache.put( pos( 4 ) );
        cache.put( pos( 6 ) );
        cache.put( pos( 8 ) );

        // when
        LogPosition lookup = cache.lookup( 6 );

        // then
        assertEquals( pos( 6 ), lookup );
    }

    @Test
    public void shouldNotReturnPositionAhead()
    {
        // given
        cache.put( pos( 4 ) );
        cache.put( pos( 6 ) );
        cache.put( pos( 8 ) );

        // when
        LogPosition lookup = cache.lookup( 7 );

        // then
        assertEquals( pos( 6 ), lookup );
    }

    @Test
    public void shouldPushOutOldEntries()
    {
        // given
        int count = PositionCache.CACHE_SIZE + 4;
        for ( int i = 0; i < count; i++ )
        {
            cache.put( pos( i ) );
        }

        // then
        for ( int i = 0; i < PositionCache.CACHE_SIZE; i++ )
        {
            int index = count - i - 1;
            assertEquals( pos( index ), cache.lookup( index ) );
        }

        int index = count - PositionCache.CACHE_SIZE - 1;
        assertEquals( FIRST, cache.lookup( index ) );
    }

    private LogPosition pos( int i )
    {
        return new LogPosition( i, 100 * i );
    }
}
