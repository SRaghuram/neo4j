package com.neo4j.bench.macro.execution.measurement;

import java.time.Duration;

public interface MeasurementControl
{
    static MeasurementControl none()
    {
        return ofCount( 0 );
    }

    static MeasurementControl single()
    {
        return ofCount( 1 );
    }

    static MeasurementControl ofCount( int maxCount )
    {
        return new CountMeasurementControl( maxCount );
    }

    static MeasurementControl ofDuration( Duration duration )
    {
        return new DurationMeasurementControl( MeasurementClock.SYSTEM, duration );
    }

    static MeasurementControl or( MeasurementControl... measurementControls )
    {
        return new OrCompositeMeasurementControl( measurementControls );
    }

    static MeasurementControl and( MeasurementControl... measurementControls )
    {
        return new AndCompositeMeasurementControl( measurementControls );
    }

    void register( long latency );

    boolean isComplete();

    void reset();

    String description();
}
