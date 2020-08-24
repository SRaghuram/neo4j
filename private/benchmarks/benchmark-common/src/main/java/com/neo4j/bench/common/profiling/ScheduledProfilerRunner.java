/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ScheduledProfilerRunner
{
    public static List<ScheduledProfiler> toScheduleProfilers( List<ExternalProfiler> externalProfilers )
    {
        return externalProfilers.stream()
                                .filter( k -> k instanceof ScheduledProfiler )
                                .map( ScheduledProfiler.class::cast )
                                .collect( Collectors.toList() );
    }

    private final ScheduledExecutorService scheduledThreadPool;

    public ScheduledProfilerRunner()
    {
        scheduledThreadPool = Executors.newScheduledThreadPool( Runtime.getRuntime().availableProcessors() );
    }

    public void submit( ScheduledProfiler profiler,
                        ForkDirectory forkDirectory,
                        ProfilerRecordingDescriptor profilerRecordingDescriptor,
                        Jvm jvm,
                        Pid pid )
    {
        scheduleProfiler( profiler,
                          forkDirectory,
                          profilerRecordingDescriptor,
                          jvm,
                          pid );
    }

    public void stop()
    {
        scheduledThreadPool.shutdown();
        try
        {
            scheduledThreadPool.awaitTermination( 1, TimeUnit.MINUTES );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "failed to stop profilers scheduler", e );
        }
    }

    public boolean isRunning()
    {
        return !scheduledThreadPool.isTerminated();
    }

    private void scheduleProfiler(
            ScheduledProfiler scheduledProfiler,
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor,
            Jvm jvm,
            Pid pid )
    {
        FixedRateValue fixedRate = getFixedRate( scheduledProfiler );
        scheduledThreadPool.scheduleAtFixedRate(
                new ScheduledProfilerRun( scheduledProfiler, forkDirectory, profilerRecordingDescriptor, jvm, pid ),
                0,
                fixedRate.period,
                fixedRate.timeUnit );
    }

    private FixedRateValue getFixedRate( ScheduledProfiler scheduledProfiler )
    {
        FixedRate fixedRate = scheduledProfiler.getClass().getAnnotation( FixedRate.class );

        if ( fixedRate == null )
        {
            fixedRate = ScheduledProfiler.class.getAnnotation( FixedRate.class );
        }
        return new FixedRateValue( fixedRate.period(), fixedRate.timeUnit() );
    }

    static class FixedRateValue
    {
        int period;
        TimeUnit timeUnit;

        FixedRateValue( int period, TimeUnit timeUnit )
        {
            this.period = period;
            this.timeUnit = timeUnit;
        }
    }

    static class ScheduledProfilerRun implements Runnable
    {

        private final ScheduledProfiler scheduledProfiler;
        private final ForkDirectory forkDirectory;
        private final ProfilerRecordingDescriptor profilerRecordingDescriptor;
        private final Jvm jvm;
        private final Pid pid;

        private Tick tick = Tick.zero();

        private ScheduledProfilerRun(
                ScheduledProfiler scheduledProfiler,
                ForkDirectory forkDirectory,
                ProfilerRecordingDescriptor profilerRecordingDescriptor,
                Jvm jvm,
                Pid pid )
        {
            super();
            this.scheduledProfiler = scheduledProfiler;
            this.forkDirectory = forkDirectory;
            this.profilerRecordingDescriptor = profilerRecordingDescriptor;
            this.jvm = jvm;
            this.pid = pid;
        }

        @Override
        public void run()
        {
            try
            {
                scheduledProfiler.onSchedule(
                        tick,
                        forkDirectory,
                        profilerRecordingDescriptor,
                        jvm,
                        pid );
            }
            finally
            {
                tick = tick.next();
            }
        }
    }
}
