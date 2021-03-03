/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import java.time.Duration;

public class MeasurementOptions
{
    private final int warmupCount;
    private final int measurementCount;
    private final Duration minMeasurementDuration;
    private final Duration maxMeasurementDuration;

    public MeasurementOptions( int warmupCount, int measurementCount, Duration minMeasurementDuration, Duration maxMeasurementDuration )
    {
        this.warmupCount = warmupCount;
        this.measurementCount = measurementCount;
        this.minMeasurementDuration = minMeasurementDuration;
        this.maxMeasurementDuration = maxMeasurementDuration;
    }

    public int warmupCount()
    {
        return warmupCount;
    }

    public int measurementCount()
    {
        return measurementCount;
    }

    public Duration minMeasurementDuration()
    {
        return minMeasurementDuration;
    }

    public Duration maxMeasurementDuration()
    {
        return maxMeasurementDuration;
    }
}
