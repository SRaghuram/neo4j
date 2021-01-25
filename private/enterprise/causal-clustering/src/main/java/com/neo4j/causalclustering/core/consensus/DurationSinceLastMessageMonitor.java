/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import java.time.Duration;

import org.neo4j.time.Stopwatch;
import org.neo4j.time.SystemNanoClock;

public class DurationSinceLastMessageMonitor implements RaftMessageTimerResetMonitor
{
    private Stopwatch lastMessage;
    private final SystemNanoClock clock;

    public DurationSinceLastMessageMonitor( SystemNanoClock clock )
    {
        this.clock = clock;
    }

    @Override
    public void timerReset()
    {
        lastMessage = clock.startStopWatch();
    }

    public Duration durationSinceLastMessage()
    {
        return lastMessage == null ? null : lastMessage.elapsed();
    }
}

