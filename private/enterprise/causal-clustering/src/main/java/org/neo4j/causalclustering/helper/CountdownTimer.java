/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import java.time.Duration;

import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofNanos;
import static org.neo4j.time.Clocks.nanoClock;

/**
 * Implementation uses {@link Clocks#nanoClock()} because it is monotonic.
 */
public class CountdownTimer
{
    private final SystemNanoClock clock;
    private long expiryTime;

    public CountdownTimer()
    {
        clock = nanoClock();
    }

    CountdownTimer( SystemNanoClock clock )
    {
        this.clock = clock;
    }

    public void set( Duration duration )
    {
        expiryTime = clock.nanos() + duration.toNanos();
    }

    public Duration timeToExpiry()
    {
        long now = clock.nanos();
        return (now >= expiryTime) ? ZERO : ofNanos( expiryTime - now );
    }

    @Override
    public String toString()
    {
        return "CountdownTimer{" + "expiryTime=" + timeToExpiry() + '}';
    }
}
