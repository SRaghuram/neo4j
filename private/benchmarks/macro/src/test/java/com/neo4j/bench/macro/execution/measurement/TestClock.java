/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

class TestClock implements MeasurementClock
{
    private long nowMillis;

    TestClock( long nowNanos )
    {
        this.nowMillis = nowNanos;
    }

    void set( long newNowMillis )
    {
        nowMillis = newNowMillis;
    }

    @Override
    public long nowMillis()
    {
        return nowMillis;
    }
}
