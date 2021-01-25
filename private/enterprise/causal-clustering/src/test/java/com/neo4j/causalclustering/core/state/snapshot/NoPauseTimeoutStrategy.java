/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import org.neo4j.internal.helpers.TimeoutStrategy;

import java.util.concurrent.atomic.AtomicInteger;

public class NoPauseTimeoutStrategy implements TimeoutStrategy
{
    private AtomicInteger invocationCount = new AtomicInteger();

    class Timeout implements TimeoutStrategy.Timeout
    {
        @Override
        public long getMillis()
        {
            return 0;
        }

        @Override
        public void increment()
        {
            invocationCount.incrementAndGet();
        }
    }

    public int invocationCount()
    {
        return invocationCount.get();
    }

    @Override
    public Timeout newTimeout()
    {
        return new Timeout();
    }
}
