/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import java.time.Duration;

public class DurationSinceLastMessageMonitor implements RaftMessageTimerResetMonitor
{
    private long lastMessageNanos = -1;

    @Override
    public void timerReset()
    {
        lastMessageNanos = System.nanoTime();
    }

    public Duration durationSinceLastMessage()
    {
        return Duration.ofNanos( System.nanoTime() - lastMessageNanos );
    }
}

