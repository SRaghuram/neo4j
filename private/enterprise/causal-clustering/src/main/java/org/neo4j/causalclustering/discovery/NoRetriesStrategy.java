/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.function.Function;
import java.util.function.Predicate;

public class NoRetriesStrategy<I, E> implements RetryStrategy<I,E>
{
    @Override
    public E apply( I input, Function<I,E> retriable, Predicate<E> shouldRetry )
    {
        return retriable.apply( input );
    }
}
