/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.checkpoint.workload;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class SyncMonitor implements Worker.Monitor
{
    private final AtomicBoolean stopSignal = new AtomicBoolean();
    private final AtomicLong transactionCounter = new AtomicLong();
    private final CountDownLatch stopLatch;

    SyncMonitor( int threads )
    {
        this.stopLatch = new CountDownLatch( threads );
    }

    @Override
    public void transactionCompleted()
    {
        transactionCounter.incrementAndGet();
    }

    @Override
    public boolean stop()
    {
        return stopSignal.get();
    }

    @Override
    public void done()
    {
        stopLatch.countDown();
    }

    public long transactions()
    {
        return transactionCounter.get();
    }

    public void stopAndWaitWorkers() throws InterruptedException
    {
        stopSignal.set( true );
        stopLatch.await();
    }
}
