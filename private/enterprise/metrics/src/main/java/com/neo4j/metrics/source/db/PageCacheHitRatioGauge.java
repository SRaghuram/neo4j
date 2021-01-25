/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Snapshot;

import java.time.Duration;

import org.neo4j.io.pagecache.monitoring.PageCacheCounters;

import static java.lang.Math.subtractExact;
import static java.time.Duration.ofMinutes;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.internal.helpers.MathUtil.portion;

class PageCacheHitRatioGauge implements Gauge<Double>
{
    private final PageCacheCounters pageCacheCounters;
    private final SlidingTimeWindowArrayReservoir hitsWindow;
    private final SlidingTimeWindowArrayReservoir missesWindow;

    PageCacheHitRatioGauge( PageCacheCounters pageCacheCounters, Duration windowDuration, Clock clock )
    {
        long windowSeconds = windowDuration.getSeconds();
        this.hitsWindow = new SlidingTimeWindowArrayReservoir( windowSeconds, SECONDS, clock );
        this.missesWindow = new SlidingTimeWindowArrayReservoir( windowSeconds, SECONDS, clock );
        this.pageCacheCounters = pageCacheCounters;
    }

    PageCacheHitRatioGauge( PageCacheCounters pageCacheCounters )
    {
        this( pageCacheCounters, ofMinutes( 1 ), Clock.defaultClock() );
    }

    @Override
    public Double getValue()
    {
        hitsWindow.update( pageCacheCounters.hits() );
        missesWindow.update( pageCacheCounters.faults() );
        long hits = getHits();
        long misses = getMisses();
        return portion( hits, misses );
    }

    private long getMisses()
    {
        Snapshot snapshot = missesWindow.getSnapshot();
        return subtractExact( snapshot.getMax(), snapshot.getMin() );
    }

    private long getHits()
    {
        Snapshot snapshot = hitsWindow.getSnapshot();
        return subtractExact( snapshot.getMax(), snapshot.getMin() );
    }
}
