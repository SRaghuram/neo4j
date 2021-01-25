/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.segmented.OpenEndRangeMap.ValueRange;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenEndRangeMapTest
{
    private OpenEndRangeMap<Integer,String> ranges = new OpenEndRangeMap<>();

    @Test
    void shouldFindNothingInEmptyMap()
    {
        assertRange( -100, 100, new ValueRange<>( null, null ) );
    }

    @Test
    void shouldFindSingleRange()
    {
        // when
        ranges.replaceFrom( 0, "A" );

        // then
        assertRange( -100, -1, new ValueRange<>( 0, null ) );
        assertRange( 0, 100, new ValueRange<>( null, "A" ) );
    }

    @Test
    void shouldHandleMultipleRanges()
    {
        // when
        ranges.replaceFrom( 0, "A" );
        ranges.replaceFrom( 5, "B" );
        ranges.replaceFrom( 10, "C" );

        // then
        assertRange( -100, -1, new ValueRange<>( 0, null ) );
        assertRange( 0, 4, new ValueRange<>( 5, "A" ) );
        assertRange( 5, 9, new ValueRange<>( 10, "B" ) );
        assertRange(   10, 100, new ValueRange<>(  null,  "C" ) );
    }

    @Test
    void shouldTruncateAtPreviousEntry()
    {
        // given
        ranges.replaceFrom( 0, "A" );
        ranges.replaceFrom( 10, "B" );

        // when
        Collection<String> removed = ranges.replaceFrom( 10, "C" );

        // then
        assertRange( -100, -1, new ValueRange<>( 0, null ) );
        assertRange(    0,   9, new ValueRange<>( 10, "A" ) );
        assertRange(   10, 100, new ValueRange<>( null, "C" ) );

        assertThat( removed, hasItems( "B" ) );
    }

    @Test
    void shouldTruncateBeforePreviousEntry()
    {
        // given
        ranges.replaceFrom( 0, "A" );
        ranges.replaceFrom( 10, "B" );

        // when
        Collection<String> removed = ranges.replaceFrom( 7, "C" );

        // then
        assertRange( -100, -1, new ValueRange<>( 0, null ) );
        assertRange(    0,   6,  new ValueRange<>( 7, "A" ) );
        assertRange(   7,  100,  new ValueRange<>( null, "C" ) );

        assertThat( removed, hasItems( "B" ) );
    }

    @Test
    void shouldTruncateSeveralEntries()
    {
        // given
        ranges.replaceFrom( 0, "A" );
        ranges.replaceFrom( 10, "B" );
        ranges.replaceFrom( 20, "C" );
        ranges.replaceFrom( 30, "D" );

        // when
        Collection<String> removed = ranges.replaceFrom( 15, "E" );

        // then
        assertRange( -100,  -1,  new ValueRange<>( 0, null ) );
        assertRange(    0,   9,  new ValueRange<>( 10, "A" ) );
        assertRange(   10,  14,  new ValueRange<>( 15, "B" ) );
        assertRange(   15, 100,  new ValueRange<>( null, "E" ) );

        assertThat( removed, hasItems( "C", "D" ) );
    }

    @Test
    void shouldOnlyPruneWholeEntries()
    {
        // given
        ranges.replaceFrom( 0, "A" );
        ranges.replaceFrom( 5, "B" );

        Collection<String> removed;

        // when / then
        removed = ranges.remove( 4 );
        assertTrue( removed.isEmpty() );

        // when
        removed = ranges.remove( 5 );
        assertFalse( removed.isEmpty() );
        assertThat( removed, hasItems( "A" ) );
    }

    private <T> void assertRange( int from, int to, ValueRange<Integer,T> expected )
    {
        for ( int i = from; i <= to; i++ )
        {
            ValueRange<Integer,String> valueRange = ranges.lookup( i );
            assertEquals( expected, valueRange );
        }
    }
}
