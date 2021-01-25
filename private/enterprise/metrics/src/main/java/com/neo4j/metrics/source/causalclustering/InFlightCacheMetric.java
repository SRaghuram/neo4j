/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheMonitor;

import java.util.concurrent.atomic.LongAdder;

public class InFlightCacheMetric implements InFlightCacheMonitor
{
    private final LongAdder misses = new LongAdder();
    private final LongAdder hits = new LongAdder();
    private volatile long totalBytes;
    private volatile long maxBytes;
    private volatile int elementCount;
    private volatile int maxElements;

    @Override
    public void miss()
    {
        misses.increment();
    }

    @Override
    public void hit()
    {
        hits.increment();
    }

    public long getMisses()
    {
        return misses.sum();
    }

    public long getHits()
    {
        return hits.sum();
    }

    public long getMaxBytes()
    {
        return maxBytes;
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getMaxElements()
    {
        return maxElements;
    }

    public long getElementCount()
    {
        return elementCount;
    }

    @Override
    public void setMaxBytes( long maxBytes )
    {
        this.maxBytes = maxBytes;
    }

    @Override
    public void setTotalBytes( long totalBytes )
    {
        this.totalBytes = totalBytes;
    }

    @Override
    public void setMaxElements( int maxElements )
    {
        this.maxElements = maxElements;
    }

    @Override
    public void setElementCount( int elementCount )
    {
        this.elementCount = elementCount;
    }
}
