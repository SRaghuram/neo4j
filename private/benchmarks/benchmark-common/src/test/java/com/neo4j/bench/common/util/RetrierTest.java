/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RetrierTest
{
    private static final double TIME_SCALE = 0.01;
    private static final Duration TIMEOUT = Duration.ofMinutes( 10 );
    private static final int RETRIES = 3;

    private final Retrier retrier = new Retrier( TIMEOUT, Clock.systemUTC(), TIME_SCALE );

    @Test
    public void shouldReturnAfterSuccess()
    {
        AtomicInteger callCount = new AtomicInteger();

        int retry = retrier.retryUntil( callCount::incrementAndGet, calls -> true, RETRIES );

        assertThat( retry, equalTo( 1 ) );
    }

    @Timeout( 5_000 )
    @Test
    void shouldRetryUntilSuccess()
    {
        AtomicInteger callCount = new AtomicInteger();

        int retry = retrier.retryUntil( callCount::incrementAndGet, calls -> calls.equals( RETRIES + 1 ), RETRIES );

        assertThat( retry, equalTo( RETRIES + 1 ) );
    }

    @Timeout( 5_000 )
    @Test
    void shouldFailWhenRetriesExceeded()
    {
        AtomicInteger callCount = new AtomicInteger();
        try
        {
            retrier.retryUntil( callCount::incrementAndGet, calls -> calls.equals( RETRIES + 2 ), RETRIES );
            fail( "Should throw" );
        }
        catch ( RuntimeException ex )
        {
            assertThat( ex.getMessage(), equalTo( "Retry limit exceeded" ) );
        }
    }

    @Timeout( 5_000 )
    @Test
    void shouldFailWhenTimeoutExceeded()
    {
        Clock clock = mock( Clock.class );
        Instant start = Instant.now();
        when( clock.instant() )
                .thenReturn( start )
                .thenReturn( start.plus( TIMEOUT ).plusSeconds( 1 ) );
        Retrier retrier = new Retrier( TIMEOUT, clock, TIME_SCALE );
        AtomicInteger callCount = new AtomicInteger();
        try
        {
            retrier.retryUntil( callCount::incrementAndGet, calls -> false, RETRIES );
            fail( "Should throw" );
        }
        catch ( RuntimeException ex )
        {
            assertThat( ex.getMessage(), equalTo( "Retry limit exceeded" ) );
        }
    }
}
