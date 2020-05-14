/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ScheduledProfilerRunner
{
    public static ScheduledProfilerRunner from( List<ExternalProfiler> externalProfilers )
    {
        List<ScheduledProfiler> schedulerProfilers =
                externalProfilers.stream().filter( k -> k instanceof ScheduledProfiler )
                        .map( ScheduledProfiler.class::cast ).collect( Collectors.toList() );
        return new ScheduledProfilerRunner( schedulerProfilers );
    }

    private final List<ScheduledProfiler> scheduledProfilers;
    private final ScheduledExecutorService scheduledThreadPool;

    private ScheduledProfilerRunner( List<ScheduledProfiler> scheduledProfilers )
    {
        this.scheduledProfilers = scheduledProfilers;
        scheduledThreadPool = Executors.newScheduledThreadPool( Runtime.getRuntime().availableProcessors() );
    }

    public void start(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters parameters,
            Jvm jvm,
            Pid pid )
    {
        scheduledProfilers.forEach( scheduledProfiler -> scheduleProfiler(
                scheduledProfiler,
                forkDirectory,
                benchmarkGroup,
                benchmark,
                parameters,
                jvm,
                pid ) );
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
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters,
            Jvm jvm,
            Pid pid )
    {
        FixedRateValue fixedRate = getFixedRate( scheduledProfiler );
        scheduledThreadPool.scheduleAtFixedRate(
                new ScheduledProfilerRun( scheduledProfiler, forkDirectory, benchmarkGroup, benchmark, additionalParameters, jvm, pid ),
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
        private final BenchmarkGroup benchmarkGroup;
        private final Benchmark benchmark;
        private final Parameters additionalParameters;
        private final Jvm jvm;
        private final Pid pid;

        private Tick tick = Tick.zero();

        private ScheduledProfilerRun(
                ScheduledProfiler scheduledProfiler,
                ForkDirectory forkDirectory,
                BenchmarkGroup benchmarkGroup,
                Benchmark benchmark,
                Parameters additionalParameters,
                Jvm jvm,
                Pid pid )
        {
            super();
            this.scheduledProfiler = scheduledProfiler;
            this.forkDirectory = forkDirectory;
            this.benchmarkGroup = benchmarkGroup;
            this.benchmark = benchmark;
            this.additionalParameters = additionalParameters;
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
                        benchmarkGroup,
                        benchmark,
                        additionalParameters,
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
