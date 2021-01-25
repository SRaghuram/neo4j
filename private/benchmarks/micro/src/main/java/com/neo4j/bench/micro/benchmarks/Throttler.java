/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import java.util.concurrent.TimeUnit;

public class Throttler
{
    private final double targetOpsPerMs;
    private boolean initialized;
    private long startTimeNs;
    private double ops;

    public Throttler( double operationsPerSecond )
    {
        this.targetOpsPerMs = operationsPerSecond / 1000D;
        this.initialized = false;
        this.startTimeNs = -1;
        this.ops = 0;
    }

    public void waitForNext()
    {
        if ( !initialized )
        {
            startTimeNs = System.nanoTime();
            initialized = true;
        }
        long ms = durationMs();
        while ( ms <= 0 )
        {
            sleep();
            ms = durationMs();
        }
        while ( ops / ms > targetOpsPerMs )
        {
            sleep();
            ms = durationMs();
        }
        ops++;
    }

    private long durationMs()
    {
        return TimeUnit.NANOSECONDS.toMillis( System.nanoTime() - startTimeNs );
    }

    private void sleep()
    {
        try
        {
            Thread.sleep( 1 );
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }
}
