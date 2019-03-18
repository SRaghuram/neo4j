/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import java.time.Duration;

import org.neo4j.time.SystemNanoClock;

public class DurationSinceLastMessageMonitor implements RaftMessageTimerResetMonitor
{
    private long lastMessageNanos = -1;
    private final SystemNanoClock clock;

    public DurationSinceLastMessageMonitor( SystemNanoClock clock )
    {
        this.clock = clock;
    }

    @Override
    public void timerReset()
    {
        lastMessageNanos = clock.nanos();
    }

    public Duration durationSinceLastMessage()
    {
        if ( lastMessageNanos == -1 )
        {
            return null;
        }
        return Duration.ofNanos( clock.nanos() - lastMessageNanos );
    }
}

