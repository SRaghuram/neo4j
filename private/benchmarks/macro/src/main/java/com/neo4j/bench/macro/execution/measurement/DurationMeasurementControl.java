package com.neo4j.bench.macro.execution.measurement;

import com.neo4j.bench.client.util.BenchmarkUtil;

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
    public void register( long latency )
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
    public String toString()
    {
        return description();
    }
}
