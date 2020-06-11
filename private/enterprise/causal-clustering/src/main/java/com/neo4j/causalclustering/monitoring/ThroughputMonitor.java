/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.monitoring;

import com.neo4j.collection.CircularBuffer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.time.Duration.between;
import static java.util.Optional.empty;

public class ThroughputMonitor extends LifecycleAdapter
{
    private final int sampleSize;
    private Duration period;
    private final ThroughputMonitorService throughputMonitorService;

    private final QualitySampler<Long> qualitySampler;
    private final CircularBuffer<Sample<Long>> samples;

    private volatile boolean isRunning;
    private final SamplesValidator samplesValidator;

    /**
     * @param lagTolerance             limits how far behind the most recent sample can be for a successful throughput measurement
     * @param period                   the desired duration period of a throughput measurement
     * @param acceptedOffset           how much smaller or larger the actual sampled window may vary from the desired sampling window
     * @param sampler                  sampler for obtaining samples
     * @param sampleSize               amount of samples to store
     * @param throughputMonitorService service of which to unregister during lifecycle stop.
     */
    ThroughputMonitor( LogProvider logProvider, Clock clock, Duration lagTolerance, Duration period, Duration acceptedOffset,
            QualitySampler<Long> sampler, int sampleSize, ThroughputMonitorService throughputMonitorService )
    {
        this.period = period;
        this.throughputMonitorService = throughputMonitorService;
        this.samplesValidator = new SamplesValidator( logProvider.getLog( getClass() ), clock, lagTolerance, period, acceptedOffset );
        this.sampleSize = sampleSize;
        this.samples = new CircularBuffer<>( sampleSize );
        this.qualitySampler = sampler;
    }

    @Override
    public void start()
    {
        throughputMonitorService.registerMonitor( this );
        isRunning = true;
    }

    @Override
    public void stop()
    {
        isRunning = false;
        throughputMonitorService.unregisterMonitor( this );
    }

    boolean samplingTask()
    {
        if ( !isRunning )
        {
            return false;
        }

        Optional<Sample<Long>> sample = qualitySampler.sample();
        sample.ifPresent( s ->
        {
            synchronized ( samples )
            {
                samples.append( s );
            }
        } );
        return sample.isPresent();
    }

    public Optional<Double> throughput()
    {
        if ( !isRunning )
        {
            return empty();
        }
        Sample<Long> first;
        Sample<Long> last;
        synchronized ( samples )
        {
            last = samples.read( sampleSize - 1 );
            if ( last == null )
            {
                return empty();
            }
            first = findClosestSample( last.instant().minus( period ) );
        }
        if ( first == null || !samplesValidator.validSamples( first, last ) )
        {
            return empty();
        }
        var dV = last.value() - first.value();
        var dT = between( first.instant(), last.instant() ).toMillis();
        var scaleToSecond = 1000.0;
        return Optional.of( dV * scaleToSecond / dT );
    }

    private Sample<Long> findClosestSample( Instant origin )
    {
        Sample<Long> closestSample = null;
        for ( int i = 0; i < samples.size(); i++ )
        {
            Sample<Long> other = samples.read( i );
            if ( other == null )
            {
                break;
            }

            if ( closestSample == null )
            {
                closestSample = other;
            }
            else
            {
                Duration currentClosest = Duration.between( closestSample.instant(), origin ).abs();
                Duration otherDistance = Duration.between( other.instant(), origin ).abs();
                if ( otherDistance.compareTo( currentClosest ) < 0 )
                {
                    closestSample = other;
                }
            }
        }
        return closestSample;
    }

    private static class SamplesValidator
    {
        private final Log log;
        private final Clock clock;
        private final Duration lagThreshold;
        private final Duration expectedPeriod;

        private final Duration acceptableOffset;

        SamplesValidator( Log log, Clock clock, Duration lagThreshold, Duration expectedPeriod, Duration acceptableOffset )
        {
            this.log = log;
            this.clock = clock;
            this.lagThreshold = lagThreshold;
            this.expectedPeriod = expectedPeriod;
            this.acceptableOffset = acceptableOffset;
        }

        boolean validSamples( Sample<Long> first, Sample<Long> last )
        {
            return lastSampleUpToDate( last ) && measuredPeriodIsAcceptable( first, last );
        }

        private boolean lastSampleUpToDate( Sample<Long> lastSample )
        {
            var durationSinceLastSample = between( lastSample.instant(), clock.instant() );
            if ( durationSinceLastSample.compareTo( lagThreshold ) > 0 )
            {
                log.warn( "Last measurement was made " + durationSinceLastSample.toMillis() + " ms ago" );
                return false;
            }
            return true;
        }

        private boolean measuredPeriodIsAcceptable( Sample<Long> first, Sample<Long> last )
        {
            var measuredPeriod = between( first.instant(), last.instant() );
            var diff = measuredPeriod.minus( expectedPeriod ).abs();
            if ( diff.compareTo( acceptableOffset ) > 0 )
            {
                log.warn( "Sampled window is not within its bound. Expected %s s, was %s s", expectedPeriod, measuredPeriod );
                return false;
            }
            return true;
        }
    }
}
