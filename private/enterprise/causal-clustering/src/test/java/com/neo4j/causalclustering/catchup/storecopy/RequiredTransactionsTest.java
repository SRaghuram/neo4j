/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static com.neo4j.causalclustering.catchup.storecopy.RequiredTransactions.noConstraint;
import static com.neo4j.causalclustering.catchup.storecopy.RequiredTransactions.requiredRange;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequiredTransactionsTest
{
    @ParameterizedTest
    @MethodSource( "invalidRanges" )
    void checkInvalidRanges( RangeProvider rangeProvider )
    {
        assertThrows( IllegalArgumentException.class, rangeProvider::get );
    }

    @ParameterizedTest
    @ValueSource( ints = {Integer.MIN_VALUE, -1} )
    void shouldNotAllowNegativeValue( int negativeInt )
    {
        assertThrows( IllegalArgumentException.class, () -> noConstraint( negativeInt ) );
    }

    @Test
    void shouldNotHaveARequiredTx()
    {
        RequiredTransactions requiredTransactions = noConstraint( 1 );

        assertTrue( requiredTransactions.noRequiredTxId() );
    }

    @Test
    void shouldHaveARequiredTx()
    {
        RequiredTransactions requiredTransactions = requiredRange( 1, 2 );

        assertFalse( requiredTransactions.noRequiredTxId() );
        assertEquals( 2, requiredTransactions.requiredTxId() );
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

        RequiredTransactions get()
        {
            return requiredRange( from, to );
        }

        @Override
        public String toString()
        {
            return "RangeProvider{" + "from=" + from + ", to=" + to + '}';
        }
    }
}
