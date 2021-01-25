/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.logging.Log;

public final class OperationProgressMonitor<T>
{
    private final Future<T> future;
    private final long inactivityTimeoutMillis;
    private final Supplier<OptionalLong> millisSinceLastResponseSupplier;
    private final Log log;

    private OperationProgressMonitor( Future<T> future, long inactivityTimeoutMillis, Supplier<OptionalLong> millisSinceLastResponseSupplier, Log log )
    {
        this.future = future;
        this.inactivityTimeoutMillis = inactivityTimeoutMillis;
        this.millisSinceLastResponseSupplier = millisSinceLastResponseSupplier;
        this.log = log;
    }

    public static <T> OperationProgressMonitor<T> of( Future<T> future, long inactivityTimeoutMillis,
            Supplier<OptionalLong> millisSinceLastResponseSupplier, Log log )
    {
        return new OperationProgressMonitor<>( future, inactivityTimeoutMillis, millisSinceLastResponseSupplier, log );
    }

    public T get() throws ExecutionException, InterruptedException, TimeoutException
    {
        long remainingTimeoutMillis = inactivityTimeoutMillis;
        while ( true )
        {
            try
            {
                return future.get( remainingTimeoutMillis, TimeUnit.MILLISECONDS );
            }
            catch ( InterruptedException | ExecutionException e )
            {
                // no need to re-interrupt because InterruptedException is rethrown
                future.cancel( false );
                throw e;
            }
            catch ( TimeoutException e )
            {
                OptionalLong millisSinceLastResponse = millisSinceLastResponseSupplier.get();
                if ( !millisSinceLastResponse.isPresent() )
                {
                    throwOnTimeout( "Request timed out with no responses after " + inactivityTimeoutMillis + " ms." );
                }
                else
                {
                    if ( millisSinceLastResponse.getAsLong() < inactivityTimeoutMillis )
                    {
                        remainingTimeoutMillis = inactivityTimeoutMillis - millisSinceLastResponse.getAsLong();
                    }
                    else
                    {
                        throwOnTimeout( "Request timed out after period of inactivity. Time since last response: " + millisSinceLastResponse + " ms." );
                    }
                }
            }
        }
    }

    private void throwOnTimeout( String message ) throws TimeoutException
    {
        log.warn( message );
        future.cancel( false );
        throw new TimeoutException( message );
    }
}
