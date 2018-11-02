/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Exponential backoff strategy helper class. Exponent is always 2.
 */
public class ExponentialBackoffStrategy implements TimeoutStrategy
{
    private final long initialBackoffTimeMillis;
    private final long upperBoundBackoffTimeMillis;

    public ExponentialBackoffStrategy( long initialBackoffTime, long upperBoundBackoffTime, TimeUnit timeUnit )
    {
        assert initialBackoffTime <= upperBoundBackoffTime;

        this.initialBackoffTimeMillis = timeUnit.toMillis( initialBackoffTime );
        this.upperBoundBackoffTimeMillis = timeUnit.toMillis( upperBoundBackoffTime );
    }

    public ExponentialBackoffStrategy( Duration initialBackoffTime, Duration upperBoundBackoffTime )
    {
        this( initialBackoffTime.toMillis(), upperBoundBackoffTime.toMillis(), TimeUnit.MILLISECONDS );
    }

    @Override
    public Timeout newTimeout()
    {
        return new Timeout()
        {
            private long backoffTimeMillis = initialBackoffTimeMillis;

            @Override
            public long getMillis()
            {
                return backoffTimeMillis;
            }

            @Override
            public void increment()
            {
                backoffTimeMillis = Math.min( backoffTimeMillis * 2, upperBoundBackoffTimeMillis );
            }
        };
    }
}
