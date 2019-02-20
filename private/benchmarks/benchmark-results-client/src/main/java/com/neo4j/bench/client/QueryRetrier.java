/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.queries.Query;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueryRetrier
{
    static final int DEFAULT_RETRY_COUNT = 10;

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
        Throwable lastException = null;
        int retry = 0;
        while ( retry <= retries )
        {
            try
            {
                return supplier.get();
            }
            catch ( Throwable e )
            {
                System.out.println( format( "Error executing callable %s\n%s", supplier, stackTraceFor( e ) ) );
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
            System.out.println( format( "Will retry after %s seconds", MILLISECONDS.toSeconds( milliseconds ) ) );
            Thread.sleep( milliseconds );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Interrupted during backoff sleep", e );
        }
    }

    private <RESULT, QUERY extends Query<RESULT>> Supplier<RESULT> querySupplier( StoreClient client, QUERY query )
    {
        return new Supplier<RESULT>()
        {
            @Override
            public RESULT get()
            {
                try
                {
                    RESULT result = client.execute( query );
                    query.nonFatalError().ifPresent( System.out::println );
                    System.out.println( "Query successfully executed: " + toString() );
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

    private String stackTraceFor( Throwable e )
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        e.printStackTrace( pw );
        return sw.toString();
    }
}
