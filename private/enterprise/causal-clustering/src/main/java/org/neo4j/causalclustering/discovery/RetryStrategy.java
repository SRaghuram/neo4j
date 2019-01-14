/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Repeats the retriable supplier until the correct result has been retrieved or the limit of retries has been
 * encountered at which point a {@link TimeoutException} is thrown.
 **/
public class RetryStrategy
{
    private final long delayInMillis;
    private final long retries;

    /**
     * @param delayInMillis number of milliseconds between each attempt at getting the desired result
     * @param retries the number of attempts to perform before giving up
     */
    public RetryStrategy( long delayInMillis, long retries )
    {
        this.delayInMillis = delayInMillis;
        this.retries = retries;
    }

    /**
     * Run a given supplier until a satisfying result is achieved
     *
     * @param action a supplier that will be executed multiple times until it returns a valid output
     * @param validator a predicate deciding if the output of the retriable supplier is valid. Assume that the function will retry if this returns false and
     * exit if it returns true
     * @param <T> the type of output of the retriable supplier
     * @return the accepted value from the supplier
     * @throws TimeoutException if maximum amount of retires is reached without an accepted value.
     */
    public <T> T apply( Supplier<T> action, Predicate<T> validator ) throws TimeoutException
    {
        T result = action.get();
        int currentIteration = 0;
        while ( !validator.test( result ) )
        {
            if ( currentIteration++ == retries )
            {
                throw new TimeoutException( "Unable to fulfill predicate within " + retries + " retries" );
            }
            try
            {
                Thread.sleep( delayInMillis );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( e );
            }
            result = action.get();
        }
        return result;
    }
}
