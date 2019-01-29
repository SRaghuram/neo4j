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
