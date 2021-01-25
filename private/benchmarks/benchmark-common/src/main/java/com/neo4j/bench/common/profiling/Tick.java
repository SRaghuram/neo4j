/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

/**
 * Used by {@link ScheduledProfiler} to indicate next invocation of profiler by scheduler.
 *
 */
public class Tick
{
    public static Tick zero()
    {
        return new Tick( 0 );
    }

    private final long counter;

    private Tick( long counter )
    {
        this.counter = counter;
    }

    public long counter()
    {
        return counter;
    }

    public Tick next()
    {
        return new Tick( counter + 1 );
    }
}
