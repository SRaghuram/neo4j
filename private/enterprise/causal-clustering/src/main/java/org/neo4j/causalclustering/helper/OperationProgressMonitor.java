/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.logging.Log;

public final class OperationProgressMonitor<T>
{
    private final Future<T> future;
    private final long inactivityTimeoutMillis;
    private final Supplier<Optional<Long>> millisSinceLastResponseSupplier;
    private final Log log;

    private OperationProgressMonitor( Future<T> future, long inactivityTimeoutMillis, Supplier<Optional<Long>> millisSinceLastResponseSupplier, Log log )
    {
        this.future = future;
        this.inactivityTimeoutMillis = inactivityTimeoutMillis;
        this.millisSinceLastResponseSupplier = millisSinceLastResponseSupplier;
        this.log = log;
    }

    public static <T> OperationProgressMonitor<T> of( Future<T> future, long inactivityTimeoutMillis,
            Supplier<Optional<Long>> millisSinceLastResponseSupplier, Log log )
    {
        return new OperationProgressMonitor<>( future, inactivityTimeoutMillis, millisSinceLastResponseSupplier, log );
    }

    public Future<T> future()
    {
        return future;
    }

    public T get() throws Exception
    {
        return get( "Operation timed out." );
    }

    public T get( String message ) throws Exception
    {
        return get( e ->  new Exception( message, e ), log );
    }

    public <E extends Throwable> T get( Function<Throwable,E> exceptionFactory, Log log ) throws E
    {
        return waitForCompletion( millisSinceLastResponseSupplier, exceptionFactory, inactivityTimeoutMillis, log );
    }

    private <E extends Throwable> T waitForCompletion( Supplier<Optional<Long>> millisSinceLastResponseSupplier,
            Function<Throwable,E> exceptionFactory, long inactivityTimeoutMillis, Log log ) throws E
    {
        long remainingTimeoutMillis = inactivityTimeoutMillis;
        while ( true )
        {
            try
            {
                return future.get( remainingTimeoutMillis, TimeUnit.MILLISECONDS );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                future.cancel( false );
                throw exceptionFactory.apply( e );
            }
            catch ( ExecutionException e )
            {
                future.cancel( false );
                throw exceptionFactory.apply( e );
            }
            catch ( TimeoutException e )
            {
                if ( !millisSinceLastResponseSupplier.get().isPresent() )
                {
                    log.warn( "Request timed out with no responses after " + inactivityTimeoutMillis + " ms." );
                    future.cancel( false );
                    throw exceptionFactory.apply( e );
                }
                else
                {
                    long millisSinceLastResponse = millisSinceLastResponseSupplier.get().get();
                    if ( millisSinceLastResponse < inactivityTimeoutMillis )
                    {
                        remainingTimeoutMillis = inactivityTimeoutMillis - millisSinceLastResponse;
                    }
                    else
                    {
                        log.warn( "Request timed out after period of inactivity. Time since last response: " + millisSinceLastResponse + " ms." );
                        future.cancel( false );
                        throw exceptionFactory.apply( e );
                    }
                }
            }
        }
    }
}
