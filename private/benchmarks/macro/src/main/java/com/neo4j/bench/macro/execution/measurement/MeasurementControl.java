/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import java.time.Duration;

import static java.time.Duration.ofSeconds;

public interface MeasurementControl
{
    static MeasurementControl compositeOf( int minCount, int minSeconds, int maxSeconds )
    {
        return and( // minimum duration
                    ofDuration( ofSeconds( minSeconds ) ),

                    // count or maximum duration, whichever happens first
                    or( // minimum number of query executions
                        ofCount( minCount ),
                        // maximum duration
                        ofDuration( ofSeconds( maxSeconds ) ) ) );
    }

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

    void register( double measurement );

    boolean isComplete();

    void reset();

    String description();
}
