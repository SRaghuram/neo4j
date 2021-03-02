/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Clock;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.monitoring.PageCacheCounters;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PageCacheHitRatioTest
{
    @Test
    void windowHitRatioEvaluation()
    {
        ManualClock manualClock = new ManualClock();
        MutableLong hits = new MutableLong();
        MutableLong misses = new MutableLong();
        ManualPageCacheCounters pageCacheCounters = new ManualPageCacheCounters( hits, misses );

        PageCacheHitRatioGauge ratioGauge = new PageCacheHitRatioGauge( pageCacheCounters, ofSeconds( 3 ), manualClock );
        assertEquals( 0, ratioGauge.getValue().doubleValue() );

        hits.increment();
        manualClock.tick();

        // hit: 0, 0, 1
        // miss: 0, 0, 0
        assertEquals( 1, ratioGauge.getValue(), 0.001 );

        hits.increment();
        manualClock.tick();

        // hit: 0, 1, 2
        // miss: 0, 0, 0
        assertEquals( 1, ratioGauge.getValue(), 0.001 );

        misses.setValue( 2 );
        manualClock.tick();

        // hit: 1, 2, 2
        // miss: 0, 0, 2
        assertEquals( 0.5, ratioGauge.getValue(), 0.001 );

        misses.setValue( 4 );
        manualClock.tick();

        // hit: 2, 2, 2
        // miss: 0, 2, 4
        assertEquals( 0.0, ratioGauge.getValue(), 0.001 );

        hits.setValue( 8 );
        manualClock.tick();

        // hit: 2, 2, 8
        // miss: 2, 4, 4
        assertEquals( 0.75, ratioGauge.getValue(), 0.01 );

        manualClock.tick();

        // hit: 2, 8, 8
        // miss: 4, 4, 4
        assertEquals( 1, ratioGauge.getValue(), 0.01 );
    }

    private static class ManualClock extends Clock
    {
        private long tick = System.nanoTime();

        void tick()
        {
            tick = tick + ofSeconds( 1 ).toNanos();
        }

        @Override
        public long getTick()
        {
            return tick;
        }
    }

    private static class ManualPageCacheCounters implements PageCacheCounters
    {

        private final MutableLong hits;
        private final MutableLong misses;

        ManualPageCacheCounters( MutableLong hits, MutableLong misses )
        {
            this.hits = hits;
            this.misses = misses;
        }

        @Override
        public long faults()
        {
            return misses.longValue();
        }

        @Override
        public long evictions()
        {
            return 0;
        }

        @Override
        public long pins()
        {
            return 0;
        }

        @Override
        public long unpins()
        {
            return 0;
        }

        @Override
        public long hits()
        {
            return hits.longValue();
        }

        @Override
        public long flushes()
        {
            return 0;
        }

        @Override
        public long merges()
        {
            return 0;
        }

        @Override
        public long bytesRead()
        {
            return 0;
        }

        @Override
        public long bytesWritten()
        {
            return 0;
        }

        @Override
        public long filesMapped()
        {
            return 0;
        }

        @Override
        public long filesUnmapped()
        {
            return 0;
        }

        @Override
        public long evictionExceptions()
        {
            return 0;
        }

        @Override
        public double hitRatio()
        {
            return 0;
        }

        @Override
        public double usageRatio()
        {
            return 0;
        }

        @Override
        public long iopqPerformed()
        {
            return 0;
        }

        @Override
        public long ioLimitedTimes()
        {
            return 0;
        }

        @Override
        public long ioLimitedMillis()
        {
            return 0;
        }
    }
}
