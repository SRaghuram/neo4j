/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.monitoring;

import com.neo4j.collection.CircularBuffer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;

public class ThroughputMonitor extends LifecycleAdapter
{
    @VisibleForTesting
    static final int SAMPLING_WINDOW_DIVISOR = 8;

    private final Log log;
    private final Clock clock;
    private final JobScheduler scheduler;
    private final Semaphore samplingTaskLock = new Semaphore( 0, true ); // Fairness means the lock is given in arrival order.

    private final Duration samplingWindow;
    private final Duration samplingInterval;

    private final QualitySampler<Long> qualitySampler;
    private final CircularBuffer<Sample<Long>> samples = new CircularBuffer<>( SAMPLING_WINDOW_DIVISOR + 1 );

    private JobHandle job;

    /**
     * @param scheduler that allows scheduled execution
     * @param samplingWindow the approximate time window over which throughput is estimated
     * @param valueSupplier obtains the value being measured
     */
    public ThroughputMonitor( LogProvider logProvider, Clock clock, JobScheduler scheduler, Duration samplingWindow, Supplier<Long> valueSupplier )
    {
        this.log = logProvider.getLog( getClass() );
        this.clock = clock;
        this.scheduler = scheduler;

        this.samplingWindow = samplingWindow;
        this.samplingInterval = samplingWindow.dividedBy( SAMPLING_WINDOW_DIVISOR );

        Duration samplingTolerance = samplingInterval.dividedBy( 2 );
        this.qualitySampler = new QualitySampler<>( clock, samplingTolerance, valueSupplier );
    }

    @Override
    public void start()
    {
        samplingTaskLock.release();
        this.job = scheduler.scheduleRecurring( Group.THROUGHPUT_MONITOR, this::samplingTask, samplingInterval.toMillis(), TimeUnit.MILLISECONDS );
    }

    @Override
    public void stop()
    {
        job.cancel();

        // After cancelling the job, no new jobs will be scheduled.
        // We drain the semaphore, blocking if necessary, to ensure that any on-going job will finish, and no enqueued jobs will start.
        samplingTaskLock.acquireUninterruptibly();
    }

    private boolean tooEarly()
    {
        if ( samples.size() == 0 )
        {
            return false;
        }

        Sample<Long> lastSample = samples.read( samples.size() - 1 );
        return Duration.between( lastSample.instant(), clock.instant() ).compareTo( samplingInterval ) < 0;
    }

    private void samplingTask()
    {
        if ( samplingTaskLock.tryAcquire() )
        {
            try
            {
                samplingTask0();
            }
            catch ( Throwable e )
            {
                log.error( "Sampling task failed exceptionally", e );
            }
            finally
            {
                samplingTaskLock.release();
            }
        }
    }

    private void samplingTask0()
    {
        if ( tooEarly() )
        {
            return;
        }

        synchronized ( this )
        {
            Optional<Sample<Long>> sample = qualitySampler.sample();

            if ( sample.isPresent() )
            {
                samples.append( sample.get() );
            }
            else
            {
                log.warn( "Sampling task failed" );
            }
        }
    }

    /**
     * Get a high quality estimate of the the average throughput over the measured window.
     * <p>
     * To get a high quality estimate we take an up-to-date sample and then go looking for another
     * older sample which fulfils some quality criteria (old enough, but not too old). If either
     * of these cannot be found, then no estimate is returned.
     *
     * @return a high quality estimate of the throughput or {@link OptionalDouble#empty()} if one could not be produced.
     */
    public synchronized Optional<Double> throughput()
    {
        Optional<Sample<Long>> optSample = qualitySampler.sample();

        if ( optSample.isEmpty() )
        {
            log.warn( "Sampling for throughput failed" );
            return Optional.empty();
        }

        Sample<Long> latestSample = optSample.get();
        Instant origin = latestSample.instant().minus( samplingWindow );
        Sample<Long> bestOldSample = findClosestSample( origin );

        if ( bestOldSample == null )
        {
            log.warn( "Throughput estimation failed due to lack of older sample" );
            return Optional.empty();
        }

        Duration acceptableDelta = samplingWindow.dividedBy( 2 );
        if ( Duration.between( bestOldSample.instant(), origin ).abs().compareTo( acceptableDelta ) > 0 )
        {
            log.warn( "Throughput estimation failed due to lack of acceptable older sample" );
            return Optional.empty();
        }

        long timeDiffMillis = Duration.between( bestOldSample.instant(), latestSample.instant() ).toMillis();
        long valueDiff = latestSample.value() - bestOldSample.value();

        double ratePerSecond = valueDiff / (double) timeDiffMillis * 1000;

        return Optional.of( ratePerSecond );
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
}
