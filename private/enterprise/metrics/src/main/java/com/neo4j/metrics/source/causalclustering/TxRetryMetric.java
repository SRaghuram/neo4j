/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.neo4j.causalclustering.catchup.tx.TxRetryMonitor;

import java.util.concurrent.atomic.AtomicLong;


class TxRetryMetric implements TxRetryMonitor
{
    private AtomicLong count = new AtomicLong( 0 );

    @Override
    public long transactionsRetries()
    {
        return count.get();
    }

    @Override
    public void retry()
    {
        count.incrementAndGet();
    }
}
