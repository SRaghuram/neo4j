/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Implementation of the RetryStrategy that repeats the retriable function until the correct result has been retrieved or the limit of retries has been
 * encountered.
 * There is a fixed delay between each retry.
 *
 * @param <INPUT> the type of input of the retriable function
 * @param <OUTPUT> the type of output of the retriable function
 */
public class MultiRetryStrategy<INPUT, OUTPUT> implements RetryStrategy<INPUT,OUTPUT>
{
    private final long delayInMillis;
    private final long retries;
    private final LogProvider logProvider;
    private final LongConsumer sleeper;

    /**
     * @param delayInMillis number of milliseconds between each attempt at getting the desired result
     * @param retries the number of attempts to perform before giving up
     * @param logProvider {@see LogProvider}
     */
    public MultiRetryStrategy( long delayInMillis, long retries, LogProvider logProvider, LongConsumer sleeper )
    {
        this.delayInMillis = delayInMillis;
        this.retries = retries;
        this.logProvider = logProvider;
        this.sleeper = sleeper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OUTPUT apply( INPUT retriableInput, Function<INPUT,OUTPUT> retriable, Predicate<OUTPUT> wasRetrySuccessful )
    {
        Log log = logProvider.getLog( MultiRetryStrategy.class );
        OUTPUT result = retriable.apply( retriableInput );
        int currentIteration = 0;
        while ( !wasRetrySuccessful.test( result ) && currentIteration++ < retries )
        {
            log.debug( "Try attempt was unsuccessful for input: %s\n", retriableInput );
            sleeper.accept( delayInMillis );
            result = retriable.apply( retriableInput );
        }
        return result;
    }
}
