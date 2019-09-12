/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.neo4j.driver.Driver;

public class PooledDriver
{
    private final Driver driver;
    private final AtomicInteger referenceCounter = new AtomicInteger();
    private final Consumer<PooledDriver> releaseCallback;
    private Instant lastUsedTimestamp;

    PooledDriver( Driver driver, Consumer<PooledDriver> releaseCallback )
    {
        this.driver = driver;
        this.releaseCallback = releaseCallback;
    }

    public Driver getDriver()
    {
        return driver;
    }

    public void release()
    {
        releaseCallback.accept( this );
    }

    AtomicInteger getReferenceCounter()
    {
        return referenceCounter;
    }

    Instant getLastUsedTimestamp()
    {
        return lastUsedTimestamp;
    }

    void setLastUsedTimestamp( Instant lastUsedTimestamp )
    {
        this.lastUsedTimestamp = lastUsedTimestamp;
    }
}
