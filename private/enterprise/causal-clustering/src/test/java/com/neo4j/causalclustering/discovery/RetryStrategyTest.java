/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.state.snapshot.NoPauseTimeoutStrategy;
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
    void numberOfTimeoutIncrementsShouldBeOneLessThanRetries() throws TimeoutException
    {
        // given
        CountingSupplier countingSupplier = new CountingSupplier();
        int retries = 5;
        NoPauseTimeoutStrategy timeoutStrategy = new NoPauseTimeoutStrategy();
        RetryStrategy subject = new RetryStrategy( timeoutStrategy, retries );

        // when
        subject.apply( countingSupplier, new ValidAfterNAttempts( 2 ) );

        // then
        assertEquals( 1, timeoutStrategy.invocationCount() );
    }

    @Test
    void successfulRetriesBreakTheRetryLoop() throws TimeoutException
    {
        CountingSupplier countingSupplier = new CountingSupplier();
        int retries = 5;
        RetryStrategy subject = new RetryStrategy( 0, retries );

        // when
        subject.apply( countingSupplier, new ValidAfterNAttempts( 2 ) );

        // then
        assertEquals( 2, countingSupplier.invocationCount() );
    }

    @Test
    void nonPositiveRetryNumberRetriesUntilSuccess() throws TimeoutException
    {
        RetryStrategy subject = new RetryStrategy( 0, 0 );
        int retries = 50;
        CountingSupplier countingSupplier = new CountingSupplier();

        // when
        Integer result = subject.apply( countingSupplier, new ValidAfterNAttempts( retries ) );

        // then no TimeoutException
        assertEquals( retries - 1, result.intValue() );
    }

    private static class ValidAfterNAttempts implements Predicate<Integer>
    {
        private int validAfter;
        private int testCount;

        ValidAfterNAttempts( int validAfter )
        {
            this.validAfter = validAfter;
        }

        @Override
        public boolean test( Integer integer )
        {
            return ++testCount >= validAfter;
        }
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
