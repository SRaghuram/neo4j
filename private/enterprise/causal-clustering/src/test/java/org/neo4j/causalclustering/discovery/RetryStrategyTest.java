/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryStrategyTest
{
    private static final Predicate<Integer> ALWAYS_VALID = i -> true;
    private static final Predicate<Integer> NEVER_VALID = i -> false;
    private static final Predicate<Integer> VALID_ON_SECOND_TIME = new Predicate<Integer>()
    {
        private boolean nextSuccessful;
        @Override
        public boolean test( Integer integer )
        {
            if ( !nextSuccessful )
            {
                nextSuccessful = true;
                return false;
            }
            return true;
        }
    };

    @Test
    void successOnRetryCausesNoDelay() throws TimeoutException
    {
        // given
        CountingSupplier countingSupplier = new CountingSupplier();
        int retries = 10;
        RetryStrategy subject = new RetryStrategy( 0, retries );

        // when
        Integer result = subject.apply( countingSupplier, ALWAYS_VALID );

        // then
        assertEquals( 1, countingSupplier.invocationCount() );
        assertEquals( 0, result.intValue(), "Function identity should be used to retrieve the expected value" );
    }

    @Test
    void numberOfIterationsDoesNotExceedMaximum()
    {
        // given
        CountingSupplier countingSupplier = new CountingSupplier();
        int retries = 5;
        RetryStrategy subject = new RetryStrategy( 0, retries );

        // when
        assertThrows( TimeoutException.class, () -> subject.apply( countingSupplier, NEVER_VALID) );

        // then
        assertEquals( retries + 1, countingSupplier.invocationCount() );
    }

    @Test
    void successfulRetriesBreakTheRetryLoop() throws TimeoutException
    {
        CountingSupplier countingSupplier = new CountingSupplier();
        int retries = 5;
        RetryStrategy subject = new RetryStrategy( 0, retries );

        // when
        subject.apply( countingSupplier, VALID_ON_SECOND_TIME );

        // then
        assertEquals( 2, countingSupplier.invocationCount() );
    }

    public static class CountingSupplier implements Supplier<Integer>
    {
        private int counter;

        int invocationCount()
        {
            return counter;
        }

        @Override
        public Integer get()
        {
            return counter++;
        }
    }

    static RetryStrategy testRetryStrategy( int numRetries )
    {
        return new RetryStrategy( 0, numRetries );
    }
}
