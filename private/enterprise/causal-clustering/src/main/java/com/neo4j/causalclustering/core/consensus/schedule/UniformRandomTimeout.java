/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A random timeout distributed uniformly in an interval.
 */
public class UniformRandomTimeout implements Timeout
{
    private final long minDelay;
    private final long maxDelay;
    private final TimeUnit unit;

    UniformRandomTimeout( long minDelay, long maxDelay, TimeUnit unit )
    {
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
        this.unit = unit;
    }

    @Override
    public Delay next()
    {
        long delay = ThreadLocalRandom.current().nextLong( minDelay, maxDelay + 1 );
        return new Delay( delay, unit );
    }
}
