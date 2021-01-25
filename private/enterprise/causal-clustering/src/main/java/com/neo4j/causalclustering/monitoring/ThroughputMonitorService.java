/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.monitoring;

import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

public class ThroughputMonitorService extends LifecycleAdapter
{
    private static final int LAG_TOLERANCE_MULTIPLIER = 2;
    public static final int SAMPLES = 10;
    public static final int TOLERANCE_DIVISOR = 2;
    private final Clock clock;
    private final JobScheduler scheduler;
    private final Collection<ThroughputMonitor> monitors = new CopyOnWriteArrayList<>();
    private final LogProvider logProvider;
    private final Duration throughputWindow;
    private final Duration sampleInterval;
    private final Log log;

    private volatile boolean isRunning;
    private JobHandle<?> jobHandle;

    public ThroughputMonitorService( Clock clock, JobScheduler scheduler, Duration throughputWindow, LogProvider logProvider )
    {
        this.throughputWindow = throughputWindow;
        this.sampleInterval = throughputWindow.dividedBy( SAMPLES );
        this.clock = clock;
        this.scheduler = scheduler;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    public ThroughputMonitor createMonitor( CommandIndexTracker commandIndexTracker )
    {
        var lagTolerance = sampleInterval.multipliedBy( LAG_TOLERANCE_MULTIPLIER );
        var acceptableOffset = throughputWindow.dividedBy( TOLERANCE_DIVISOR );
        return new ThroughputMonitor( logProvider, clock, lagTolerance, throughputWindow, acceptableOffset,
                new QualitySampler<>( clock, sampleInterval.dividedBy( TOLERANCE_DIVISOR ), commandIndexTracker::getAppliedCommandIndex ), SAMPLES, this );
    }

    void registerMonitor( ThroughputMonitor throughputMonitor )
    {
        monitors.add( throughputMonitor );
    }

    void unregisterMonitor( ThroughputMonitor throughputMonitor )
    {
        monitors.remove( throughputMonitor );
    }

    @Override
    public void start() throws Exception
    {
        isRunning = true;
        jobHandle = scheduler.scheduleRecurring( Group.THROUGHPUT_MONITOR, this::sample, sampleInterval.toMillis(), TimeUnit.MILLISECONDS );
    }

    @Override
    public void stop()
    {
        isRunning = false;
        jobHandle.cancel();
    }

    private void sample()
    {
        if ( isRunning )
        {
            int failed = 0;
            for ( ThroughputMonitor monitor : monitors )
            {
                if ( !monitor.samplingTask() )
                {
                    failed++;
                }
            }
            if ( failed != 0 )
            {
                log.warn( "Failed to sample %d out of %d sampling tasks", failed, monitors.size() );
            }
        }
    }
}
