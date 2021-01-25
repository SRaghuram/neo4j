/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import com.neo4j.cc_robustness.util.Duration;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TimedCondition implements Condition
{
    private final long end;

    TimedCondition( Duration duration )
    {
        end = nanoTime() + duration.to( NANOSECONDS );
    }

    @Override
    public boolean met()
    {
        return nanoTime() >= end;
    }
}
