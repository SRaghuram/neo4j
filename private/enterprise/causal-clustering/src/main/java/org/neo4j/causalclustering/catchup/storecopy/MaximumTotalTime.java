/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.neo4j.time.Clocks;

import static java.lang.String.format;

public class MaximumTotalTime implements TerminationCondition
{
    private final long endTime;
    private final Clock clock;
    private long time;
    private TimeUnit timeUnit;

    public MaximumTotalTime( long time, TimeUnit timeUnit )
    {
        this( time, timeUnit, Clocks.systemClock() );
    }

    MaximumTotalTime( long time, TimeUnit timeUnit, Clock clock )
    {
        this.endTime = clock.millis() + timeUnit.toMillis( time );
        this.clock = clock;
        this.time = time;
        this.timeUnit = timeUnit;
    }

    @Override
    public void assertContinue() throws StoreCopyFailedException
    {
        if ( clock.millis() > endTime )
        {
            throw new StoreCopyFailedException( format( "Maximum time passed %d %s. Not allowed to continue", time, timeUnit ) );
        }
    }
}
