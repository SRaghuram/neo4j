/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.concurrent.atomic.AtomicLong;

public class DatabaseOperationCounter
{
    private AtomicLong createCount = new AtomicLong( 0 );
    private AtomicLong startCount = new AtomicLong( 0 );
    private AtomicLong stopCount = new AtomicLong( 0 );
    private AtomicLong dropCount = new AtomicLong( 0 );
    private AtomicLong failedCount = new AtomicLong( 0 );
    private AtomicLong recoveredCount = new AtomicLong( 0 );

    public long startCount()
    {
        return startCount.get();
    }

    public long createCount()
    {
        return createCount.get();
    }

    public long stopCount()
    {
        return stopCount.get();
    }

    public long dropCount()
    {
        return dropCount.get();
    }

    public long failedCount()
    {
        return failedCount.get();
    }

    public long recoveredCount()
    {
        return recoveredCount.get();
    }

    public void increaseCreateCount()
    {
        createCount.incrementAndGet();
    }

    public void increaseStartCount()
    {
        startCount.incrementAndGet();
    }

    public void increaseStopCount()
    {
        stopCount.incrementAndGet();
    }

    public void increaseDropCount()
    {
        dropCount.incrementAndGet();
    }

    public void increaseFailedCount()
    {
        failedCount.incrementAndGet();
    }

    public void increaseRecoveredCount()
    {
        recoveredCount.incrementAndGet();
    }
}
