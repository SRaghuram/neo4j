/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class Result
{
    private final long scheduledStartUtc;
    private final long startUtc;
    private final long duration;
    private final long rows;
    private final TimeUnit timeUnit;

    Result( long scheduledStartUtc, long startUtc, long duration, TimeUnit timeUnit, long rows )
    {
        this.scheduledStartUtc = scheduledStartUtc;
        this.startUtc = startUtc;
        this.duration = duration;
        this.rows = rows;
        this.timeUnit = timeUnit;
    }

    public long scheduledStartUtc()
    {
        return scheduledStartUtc;
    }

    public long startUtc()
    {
        return startUtc;
    }

    public long duration()
    {
        return duration;
    }

    public long duration( TimeUnit toTimeUnit )
    {
        return toTimeUnit.convert( duration, timeUnit );
    }

    public long rows()
    {
        return rows;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        Result result = (Result) o;
        return scheduledStartUtc == result.scheduledStartUtc &&
               startUtc == result.startUtc &&
               duration == result.duration &&
               rows == result.rows;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( scheduledStartUtc, startUtc, duration, rows );
    }

    @Override
    public String toString()
    {
        return "[" + +scheduledStartUtc + "," + startUtc + "," + duration + "," + rows + "]";
    }
}
