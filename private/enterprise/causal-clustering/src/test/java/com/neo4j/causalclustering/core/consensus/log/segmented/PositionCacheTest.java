/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.LogPosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PositionCacheTest
{
    private final long recordOffset = SegmentHeader.CURRENT_RECORD_OFFSET;
    private final PositionCache cache = new PositionCache( recordOffset );
    private final LogPosition FIRST = new LogPosition( 0, recordOffset );

    @Test
    void shouldReturnSaneDefaultPosition()
    {
        // when
        LogPosition position = cache.lookup( 5 );

        // then
        Assertions.assertEquals( FIRST, position );
    }

    @Test
    void shouldReturnBestPosition()
    {
        // given
        cache.put( pos( 4 ) );
        cache.put( pos( 6 ) );

        // when
        LogPosition lookup = cache.lookup( 7 );

        // then
        Assertions.assertEquals( pos( 6 ), lookup );
    }

    @Test
    void shouldReturnExactMatch()
    {
        // given
        cache.put( pos( 4 ) );
        cache.put( pos( 6 ) );
        cache.put( pos( 8 ) );

        // when
        LogPosition lookup = cache.lookup( 6 );

        // then
        Assertions.assertEquals( pos( 6 ), lookup );
    }

    @Test
    void shouldNotReturnPositionAhead()
    {
        // given
        cache.put( pos( 4 ) );
        cache.put( pos( 6 ) );
        cache.put( pos( 8 ) );

        // when
        LogPosition lookup = cache.lookup( 7 );

        // then
        Assertions.assertEquals( pos( 6 ), lookup );
    }

    @Test
    void shouldPushOutOldEntries()
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
            Assertions.assertEquals( pos( index ), cache.lookup( index ) );
        }

        int index = count - PositionCache.CACHE_SIZE - 1;
        Assertions.assertEquals( FIRST, cache.lookup( index ) );
    }

    private LogPosition pos( int i )
    {
        return new LogPosition( i, 100 * i );
    }
}
