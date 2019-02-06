/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

public class CountMeasurementControl implements MeasurementControl
{
    private final long maxCount;
    private long count;

    CountMeasurementControl( long maxCount )
    {
        this.maxCount = maxCount;
        this.count = 0;
    }

    @Override
    public void register( long latency )
    {
        count++;
    }

    @Override
    public boolean isComplete()
    {
        return count >= maxCount;
    }

    @Override
    public void reset()
    {
        count = 0;
    }

    @Override
    public String description()
    {
        return "count( " + maxCount + " )";
    }

    @Override
    public String toString()
    {
        return description();
    }
}
