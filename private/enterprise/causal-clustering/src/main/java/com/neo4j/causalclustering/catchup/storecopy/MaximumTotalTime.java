/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.time.Clock;
import java.time.Duration;


import static java.lang.String.format;

public class MaximumTotalTime implements TerminationCondition
{
    private final long endTime;
    private final Clock clock;
    private final Duration duration;

    MaximumTotalTime( Duration duration, Clock clock )
    {
        this.duration = duration;
        this.endTime = clock.millis() + duration.toMillis();
        this.clock = clock;
    }

    @Override
    public void assertContinue() throws StoreCopyFailedException
    {
        if ( clock.millis() > endTime )
        {
            throw new StoreCopyFailedException( format( "Maximum time passed %s. Not allowed to continue", duration ) );
        }
    }
}
