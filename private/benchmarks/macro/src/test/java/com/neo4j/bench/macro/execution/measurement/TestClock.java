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
