/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LongRangeTest
{
    @Test
    void shouldBeWithinRange()
    {
        int from = 4;
        int to = 8;
        LongRange range = LongRange.range( from, to );

        assertFalse( range.isWithinRange( from - 1 ) );
        assertFalse( range.isWithinRange( to + 1 ) );
        for ( int i = from; i < to + 1; i++ )
        {
            assertTrue( range.isWithinRange( i ) );
        }
    }

    @ParameterizedTest
    @MethodSource( "validRanges" )
    void shouldBeWithinRange( RangeProvider rangeProvider )
    {
        Assertions.assertDoesNotThrow( rangeProvider::get );
    }

    @ParameterizedTest
    @MethodSource( "invalidRanges" )
    void checkInvalidRanges( RangeProvider rangeProvider )
    {
        assertThrows( IllegalArgumentException.class, rangeProvider::get );
    }

    private static Stream<RangeProvider> invalidRanges()
    {
        return Stream.of( new RangeProvider( 1, 0 ), new RangeProvider( Long.MAX_VALUE, Long.MAX_VALUE + 1 ), new RangeProvider( -1, 0 ) );
    }

    private static Stream<RangeProvider> validRanges()
    {
        return Stream.of( new RangeProvider( 0, 0 ), new RangeProvider( Long.MAX_VALUE, Long.MAX_VALUE ), new RangeProvider( 0, Long.MAX_VALUE ) );
    }

    static class RangeProvider
    {
        private final long from;
        private final long to;

        RangeProvider( long from, long to )
        {
            this.from = from;
            this.to = to;
        }

        LongRange get()
        {
            return LongRange.range( from, to );
        }

        @Override
        public String toString()
        {
            return "RangeProvider{" + "from=" + from + ", to=" + to + '}';
        }
    }
}
