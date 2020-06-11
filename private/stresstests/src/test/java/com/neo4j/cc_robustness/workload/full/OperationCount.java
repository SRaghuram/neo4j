/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class OperationCount
{
    private final AtomicInteger count = new AtomicInteger();
    private long time;

    OperationCount()
    {
        ping();
    }

    void increment()
    {
        ping();
        count.incrementAndGet();
    }

    void ping()
    {
        time = nanoTime();
    }

    int secondsSinceLastIncremented()
    {
        return (int) NANOSECONDS.toSeconds( nanoTime() - time );
    }

    @Override
    public String toString()
    {
        return "" + count.get();
    }

    int count()
    {
        return count.get();
    }
}
