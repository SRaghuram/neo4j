/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ThrottlerIT
{
    static final double PERCENTAGE_TOLERANCE = 0.01;
    static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat( "###,###,###,##0.00" );
    static final int EXPERIMENT_DURATION = 5; // seconds

    @Test
    public void shouldThrottleToConfiguredThroughput() throws IOException, InterruptedException
    {
        List<Integer> targetOpPerSecValues = Lists.newArrayList( 100, 100_000 );
        for ( int targetOpPerSec : targetOpPerSecValues )
        {
            TestThread thread = new TestThread( targetOpPerSec );
            long startTime = System.currentTimeMillis();
            thread.start();
            Thread.sleep( TimeUnit.SECONDS.toMillis( EXPERIMENT_DURATION ) );
            thread.stopThread();
            thread.join();
            long durationMs = System.currentTimeMillis() - startTime;
            double actualOpPerSec = ((double) thread.count / (double) durationMs) * 1000D;
            String description = format( "Target: %s (op/s), Actual: %s (op/s)",
                                         DECIMAL_FORMAT.format( targetOpPerSec ),
                                         DECIMAL_FORMAT.format( actualOpPerSec ) );
            assertTrue( areSimilar( targetOpPerSec, actualOpPerSec, PERCENTAGE_TOLERANCE ), description );
        }
    }

    private boolean areSimilar( double first, double second, double percentageTolerance )
    {
        if ( first > second )
        {
            return 1 - second / first < percentageTolerance;
        }
        else
        {
            return 1 - first / second < percentageTolerance;
        }
    }

    private static class TestThread extends Thread
    {
        final Throttler throttler;
        boolean stop;
        long count;

        TestThread( int operationsPerSecond )
        {
            this.throttler = new Throttler( operationsPerSecond );
            this.count = 0;
        }

        @Override
        public void run()
        {
            while ( !stop )
            {
                count++;
                throttler.waitForNext();
            }
        }

        void stopThread()
        {
            stop = true;
        }
    }
}
