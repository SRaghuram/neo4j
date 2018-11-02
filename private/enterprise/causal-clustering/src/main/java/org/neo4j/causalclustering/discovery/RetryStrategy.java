/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A strategy pattern for deciding how retries will be handled.
 * <p>
 * Depending on the implementation, it is assumed the retriable function will be executed until conditions satisfying desired output are met and then the latest
 * (or most valid)
 * result will be returned
 * </p>
 *
 * @param <I> Type of input used for the input function (assumes 1-parameter input functions)
 * @param <E> Type of output returned from retriable function
 */
public interface RetryStrategy<I, E>
{
    /**
     * Run a given function until a satisfying result is achieved
     *
     * @param input the input parameter that is given to the retriable function
     * @param retriable a function that will be executed multiple times until it returns a valid output
     * @param shouldRetry a predicate deciding if the output of the retriable function is valid. Assume that the function will retry if this returns false and
     * exit if it returns true
     * @return the latest (or most valid) output of the retriable function, depending on implementation
     */
    E apply( I input, Function<I,E> retriable, Predicate<E> shouldRetry );
}
