/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.neo4j.causalclustering.core.consensus.log.cache.CircularBuffer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Used for measuring a value over time and getting a throughput metric
 */
public class PollingThroughputMonitor
{
    public static final Duration DEFAULT_REPORTED_PERIOD = Duration.ofSeconds( 1 );
    public static final int DEFAULT_NUMBER_OF_MEASUREMENTS = 60;

    private final double minIntervalThresholdSeconds;

    private final CircularBuffer<TimestampedValue> recordedValues;
    private volatile TimestampedValue oldestValue;
    private volatile TimestampedValue latestValue;

    private final Log log;

    /**
     * @param numberOfMeasurements Collected number of measurements. Larger values make the throughput less affected by spikes in progress.
     * @param measurementInterval The duration that we want to report throughput over. If this is 1 second, then the throughput will be over 1 second.
     * @param jobScheduler JobScheduler that allows scheduled execution
     * @param valueSupplier obtains the value being measured
     */
    public PollingThroughputMonitor( LogProvider logProvider, SystemNanoClock clock, int numberOfMeasurements, Duration measurementInterval,
            JobScheduler jobScheduler, LongSupplier valueSupplier )
    {
        if ( measurementInterval.toMillis() == 0 )
        {
            throw new IllegalArgumentException( "Reported period cannot be 0ms" );
        }
        if ( numberOfMeasurements < 2 )
        {
            throw new IllegalArgumentException( "There must be at least 2 measurements to report throughput. Actual = " + numberOfMeasurements );
        }

        this.log = logProvider.getLog( PollingThroughputMonitor.class );
        recordedValues = new CircularBuffer<>( numberOfMeasurements );

        // When measuring throughput, we need at least 2 values. The total duration of this minimum condition is going to be the measurementInterval.
        // If the recorded measurements for throughput are more frequent than minIntervalThresholdSeconds,
        // we can avoid some boundary cases (such as divide by zero)
        double minIntervalThresholdNanos = measurementInterval.toNanos();
        this.minIntervalThresholdSeconds = minIntervalThresholdNanos / SECONDS.toNanos( 1 );

        PauseTolerantSupplier pauseTolerantSupplier = new PauseTolerantSupplier( logProvider, clock, valueSupplier );
        schedule( jobScheduler, pauseTolerantSupplier, measurementInterval );
    }

    /**
     * Add a polling job to the provided scheduler
     */
    private void schedule( JobScheduler jobScheduler, PauseTolerantSupplier valueSupplier, Duration reportedPeriod )
    {
        long milliPeriod = reportedPeriod.toMillis();
        Runnable recordThroughputRunnable = () ->
        {
            Optional<TimestampedValue> measurement = valueSupplier.get();
            measurement.ifPresent( this::updateNextEntry );
        };
        jobScheduler.scheduleRecurring( Group.THROUGHPUT_MONITOR, recordThroughputRunnable, milliPeriod, TimeUnit.MILLISECONDS );
    }

    /**
     * Report the average throughput over the measured window
     *
     * @return throughput per second
     */
    public OptionalDouble throughput()
    {
        if ( latestValue == null || latestValue == oldestValue )
        {
            return OptionalDouble.empty();
        }
        double valueChange = latestValue.getValue() - oldestValue.getValue();
        long rawDurationNanos =
                latestValue.getTimeUnit().toNanos( latestValue.getTimestamp() ) - oldestValue.getTimeUnit().toNanos( oldestValue.getTimestamp() );
        double rawDuration = rawDurationNanos / (double) SECONDS.toNanos( 1 );

        if ( rawDuration < minIntervalThresholdSeconds )
        {
            return OptionalDouble.empty();
        }

        double rawThroughputPerSecond = valueChange / rawDuration;
        return OptionalDouble.of( rawThroughputPerSecond );
    }

    private synchronized void updateNextEntry( TimestampedValue value )
    {
        latestValue = value;

        TimestampedValue removed = recordedValues.append( value );
        if ( removed != null )
        {
            oldestValue = removed;
        }
        else
        {
            oldestValue = oldestValue == null ? value : oldestValue;
        }
    }
}
