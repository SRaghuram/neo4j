/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static com.neo4j.causalclustering.catchup.storecopy.RequiredTransactionRange.single;

class RequiredTransactionRangeTest
{
    @ParameterizedTest
    @MethodSource( "invalidRanges" )
    void checkInvalidRanges( RangeProvider rangeProvider )
    {
        Assertions.assertThrows( IllegalArgumentException.class, rangeProvider::get );
    }

    @Test
    void shouldNotAllowNegativeValue()
    {
        Assertions.assertThrows( IllegalArgumentException.class, () -> single( -1 ) );
    }

    @Test
    void shouldBeWithinRange()
    {
        int from = 4;
        int to = 8;
        RequiredTransactionRange range = RequiredTransactionRange.range( from, to );

        assertFalse( range.withinRange( from - 1 ) );
        assertFalse( range.withinRange( to + 1 ) );
        for ( int i = from; i < to + 1; i++ )
        {
            assertTrue( range.withinRange( i ) );
        }
    }

    @Test
    void shouldBeWithinRangeForSingle()
    {
        int single = 4;
        RequiredTransactionRange range = RequiredTransactionRange.single( single );

        assertFalse( range.withinRange( single - 1 ) );
        assertFalse( range.withinRange( single + 1 ) );
        assertTrue( range.withinRange( single ) );
    }

    private static Stream<RangeProvider> invalidRanges()
    {
        return Stream.of( new RangeProvider( 1, 0 ), new RangeProvider( Long.MAX_VALUE, Long.MAX_VALUE + 1 ), new RangeProvider( -1, 0 ) );
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

        RequiredTransactionRange get()
        {
            return RequiredTransactionRange.range( from, to );
        }

        @Override
        public String toString()
        {
            return "RangeProvider{" + "from=" + from + ", to=" + to + '}';
        }
    }
}
