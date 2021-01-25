/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import com.neo4j.bench.common.util.BenchmarkUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.time.Duration;

public class DurationMeasurementControl implements MeasurementControl
{
    private final MeasurementClock clock;
    private final long maxDurationMs;
    private long startMs;

    DurationMeasurementControl( MeasurementClock clock, Duration maxDuration )
    {
        this.clock = clock;
        this.maxDurationMs = maxDuration.toMillis();
        this.startMs = -1;
    }

    @Override
    public void register( double measurement )
    {
    }

    @Override
    public boolean isComplete()
    {
        return clock.nowMillis() - startMs > maxDurationMs;
    }

    @Override
    public void reset()
    {
        startMs = clock.nowMillis();
    }

    @Override
    public String description()
    {
        return "time( " + BenchmarkUtil.durationToString( Duration.ofMillis( maxDurationMs ) ) + " )";
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public boolean equals( Object obj )
    {
        return EqualsBuilder.reflectionEquals( this, obj );
    }

    @Override
    public String toString()
    {
        return description();
    }
}
