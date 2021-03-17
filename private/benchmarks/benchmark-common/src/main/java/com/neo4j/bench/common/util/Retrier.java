/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Retrier
{
    private static final Logger LOG = LoggerFactory.getLogger( Retrier.class );
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes( 5 );

    private final Duration timeout;
    private final Clock clock;
    private final double timeScale;

    public Retrier()
    {
        this( DEFAULT_TIMEOUT );
    }

    public Retrier( Duration timeout )
    {
        this( timeout, Clock.systemUTC(), 1 );
    }

    @VisibleForTesting
    Retrier( Duration timeout, Clock clock, double timeScale )
    {
        this.timeout = timeout;
        this.clock = clock;
        this.timeScale = timeScale;
    }

    public <R> R retryUntil( Supplier<R> supplier, Predicate<R> predicate, int retries )
    {
        Instant deadline = clock.instant().plus( timeout );
        int retry = 0;
        while ( retry <= retries && clock.instant().isBefore( deadline ) )
        {
            R result = supplier.get();
            if ( predicate.test( result ) )
            {
                return result;
            }
            backOff( retry++ );
        }
        throw new RuntimeException( "Retry limit exceeded" );
    }

    public <R> R retry( Supplier<R> supplier, int retries )
    {
        Throwable lastException = null;
        Instant deadline = clock.instant().plus( timeout );
        int retry = 0;
        while ( retry <= retries && clock.instant().isBefore( deadline ) )
        {
            try
            {
                return supplier.get();
            }
            catch ( Throwable e )
            {
                LOG.error( "Error executing callable {}", supplier, e );
                backOff( retry++ );
                lastException = e;
            }
        }
        throw new RuntimeException( "Failed to successfully execute callable", lastException );
    }

    private void backOff( int retry )
    {
        try
        {
            // retry:                   0  1  2  3   4   5   6    7    8    9    10    11    12    13
            // milliseconds (2^retry):  1  2  4  8  16  32  64  128  256  512  1024  2048  4096  8192

            // add 10 to 'retry' so minimum sleep duration is 1 second, and limit it to 10 seconds
            long milliseconds = Math.min( Math.round( Math.pow( 2, retry + 10 ) * timeScale ), TimeUnit.SECONDS.toMillis( 10 ) );
            LOG.warn( String.format( "Will retry after %.1f seconds", milliseconds / 1000d ) );
            Thread.sleep( milliseconds );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Interrupted during backoff sleep", e );
        }
    }
}
