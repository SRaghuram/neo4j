/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.causalclustering.catchup.tx.TxPullRequestsMonitor;

import java.util.concurrent.atomic.AtomicLong;

class TxPullRequestsMetric implements TxPullRequestsMonitor
{
    private AtomicLong count = new AtomicLong( 0 );

    @Override
    public long txPullRequestsReceived()
    {
        return count.get();
    }

    @Override
    public void increment()
    {
        count.incrementAndGet();
    }
}
