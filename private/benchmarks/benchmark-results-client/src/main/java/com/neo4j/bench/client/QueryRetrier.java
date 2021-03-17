/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Retrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;

public class QueryRetrier
{

    private static final Logger LOG = LoggerFactory.getLogger( QueryRetrier.class );

    static final int DEFAULT_RETRY_COUNT = 10;
    public static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes( 5 );

    private final boolean verbose;
    private final Retrier retrier;

    public QueryRetrier( boolean verbose )
    {
        this( verbose, DEFAULT_TIMEOUT );
    }

    public QueryRetrier( boolean verbose, Duration timeout )
    {
        this.verbose = verbose;
        this.retrier = new Retrier( timeout );
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
        return retrier.retry( supplier, retries );
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
