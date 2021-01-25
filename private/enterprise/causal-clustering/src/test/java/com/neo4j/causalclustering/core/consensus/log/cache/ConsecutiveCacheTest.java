/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsecutiveCacheTest
{
    private ConsecutiveCache<Integer> cache;
    private Integer[] evictions;

    private void setEvictionsAndCache( int capacity )
    {
        this.evictions = new Integer[capacity];
        this.cache = new ConsecutiveCache<>( capacity );
    }

    static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{{1}, {2}, {3}, {4}, {8}, {1024}} );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void testEmptyInvariants( int capacity )
    {
        setEvictionsAndCache( capacity );

        assertEquals( 0, cache.size() );
        for ( int i = 0; i < capacity; i++ )
        {
            assertNull( cache.get( i ) );
        }
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void testCacheFill( int capacity )
    {
        setEvictionsAndCache( capacity );

        for ( int i = 0; i < capacity; i++ )
        {
            // when
            cache.put( i, i, evictions );
            assertTrue( Stream.of( evictions ).allMatch( Objects::isNull ) );

            // then
            assertEquals( i + 1, cache.size() );
        }

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            assertEquals( i, cache.get( i ).intValue() );
        }
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void testCacheMultipleFills( int capacity )
    {
        setEvictionsAndCache( capacity );

        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        for ( int i = capacity; i < 10 * capacity; i++ )
        {
            // when
            cache.put( i, i, evictions );

            // then
            assertEquals( i - capacity, evictions[0].intValue() );
            assertTrue( Stream.of( evictions ).skip( 1 ).allMatch( Objects::isNull ) );
            assertEquals( capacity, cache.size() );
        }
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void testCacheClearing( int capacity )
    {
        setEvictionsAndCache( capacity );

        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        cache.clear( evictions );

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            assertEquals( i, evictions[i].intValue() );
            assertNull( cache.get( i ) );
        }

        assertEquals( 0, cache.size() );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void testEntryOverride( int capacity )
    {
        setEvictionsAndCache( capacity );

        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        cache.put( capacity / 2, 10000, evictions );

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            if ( i == capacity / 2 )
            {
                continue;
            }

            assertEquals( i, evictions[i].intValue() );
            assertNull( cache.get( i ) );
        }

        assertEquals( 10000, cache.get( capacity / 2 ).intValue() );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void testEntrySkip( int capacity )
    {
        setEvictionsAndCache( capacity );

        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        cache.put( capacity + 1, 10000, evictions );

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            assertEquals( i, evictions[i].intValue() );
            assertNull( cache.get( i ) );
        }

        assertEquals( 10000, cache.get( capacity + 1 ).intValue() );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void testPruning( int capacity )
    {
        setEvictionsAndCache( capacity );

        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        int upToIndex = capacity / 2;
        cache.prune( upToIndex, evictions );

        // then
        for ( int i = 0; i <= upToIndex; i++ )
        {
            assertNull( cache.get( i ) );
            assertEquals( i, evictions[i].intValue() );
        }

        for ( int i = upToIndex + 1; i < capacity; i++ )
        {
            assertEquals( i, cache.get( i ).intValue() );
        }
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void testRemoval( int capacity )
    {
        setEvictionsAndCache( capacity );

        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // then
        for ( int i = 0; i < capacity; i++ )
        {
            // when
            Integer removed = cache.remove();

            // then
            assertEquals( i, removed.intValue() );
        }

        assertNull( cache.remove() );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void testTruncation( int capacity )
    {
        setEvictionsAndCache( capacity );

        // given
        for ( int i = 0; i < capacity; i++ )
        {
            cache.put( i, i, evictions );
        }

        // when
        int fromIndex = capacity / 2;
        cache.truncate( fromIndex, evictions );

        // then
        for ( int i = 0; i < fromIndex; i++ )
        {
            assertEquals( i, cache.get( i ).intValue() );
        }

        for ( int i = fromIndex; i < capacity; i++ )
        {
            assertNull( cache.get( i ) );
            assertEquals( i, evictions[capacity - i - 1].intValue() );
        }
    }
}
