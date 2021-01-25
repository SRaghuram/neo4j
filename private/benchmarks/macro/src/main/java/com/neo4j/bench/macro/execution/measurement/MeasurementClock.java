/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

public interface MeasurementClock
{
    MeasurementClock SYSTEM = new SystemClock();

    long nowMillis();

    class SystemClock implements MeasurementClock
    {
        @Override
        public long nowMillis()
        {
            return System.currentTimeMillis();
        }
    }
}
