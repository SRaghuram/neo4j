/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.queries.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueryRetrier
{

    private static final Logger LOG = LoggerFactory.getLogger( QueryRetrier.class );

    static final int DEFAULT_RETRY_COUNT = 10;
    public static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes( 5 );

    private final boolean verbose;
    private final Duration timeout;

    public QueryRetrier( boolean verbose )
    {
        this( verbose, DEFAULT_TIMEOUT );
    }

    public QueryRetrier( boolean verbose, Duration timeout )
    {
        this.verbose = verbose;
        this.timeout = timeout;
    }

    public <QUERY extends Query<RESULT>, RESULT> RESULT execute( StoreClient client, QUERY query )
    {
        return execute( client, query, DEFAULT_RETRY_COUNT );
    }

    public <QUERY extends Query<RESULT>, RESULT> RESULT execute( StoreClient client, QUERY query, int retries )
    {
        return retry( querySupplier( client, query ), retries );
    }

    <R> R retry( Supplier<R> supplier, int retries )
    {
        Instant start = Instant.now();
        Throwable lastException = null;
        int retry = 0;
        while ( retry <= retries &&
                Instant.now().isBefore( start.plus( timeout ) ) )
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

            // add 10 to 'retry' so minimum sleep duration is 1 second
            long milliseconds = Math.round( Math.pow( 2, retry + 10 ) );
            LOG.warn( "Will retry after {}} seconds", MILLISECONDS.toSeconds( milliseconds ) );
            Thread.sleep( milliseconds );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Interrupted during backoff sleep", e );
        }
    }

    private <RESULT, QUERY extends Query<RESULT>> Supplier<RESULT> querySupplier( StoreClient client, QUERY query )
    {
        return new Supplier<>()
        {
            @Override
            public RESULT get()
            {
                try
                {
                    RESULT result = client.execute( query );
                    query.nonFatalError().ifPresent( LOG::warn );
                    if ( verbose )
                    {
                        LOG.debug( "Query successfully executed: " + toString() );
                    }
                    return result;
                }
                catch ( Throwable e )
                {
                    client.reconnect();
                    throw e;
                }
            }

            @Override
            public String toString()
            {
                return query.getClass().getSimpleName();
            }
        };
    }
}
