/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.causalclustering.catchup.tx.PullRequestMonitor;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

class PullRequestMetric implements PullRequestMonitor
{
    private AtomicLong lastRequestedTxId = new AtomicLong( 0 );
    private AtomicLong lastReceivedTxId = new AtomicLong( 0 );
    private LongAdder events = new LongAdder(  );

    @Override
    public void txPullRequest( long txId )
    {
        events.increment();
        this.lastRequestedTxId.set( txId );
    }

    @Override
    public void txPullResponse( long txId )
    {
        lastReceivedTxId.set( txId );
    }

    @Override
    public long lastRequestedTxId()
    {
        return this.lastRequestedTxId.get();
    }

    @Override
    public long numberOfRequests()
    {
        return events.longValue();
    }

    @Override
    public long lastReceivedTxId()
    {
        return lastReceivedTxId.get();
    }
}
