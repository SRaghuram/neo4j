/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.ScheduledProfiler.FixedRate;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

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
        Method intfMethod = findIntfMethod();
        FixedRate defaultFixedRate = intfMethod.getAnnotation( FixedRate.class );
        try
        {
            Method method =
                    scheduledProfiler.getClass().getMethod( intfMethod.getName(), intfMethod.getParameterTypes() );
            FixedRate fixedRate = method.getAnnotation( ScheduledProfiler.FixedRate.class );
            if ( fixedRate == null )
            {
                return new FixedRateValue( defaultFixedRate.period(), defaultFixedRate.timeUnit() );
            }
            else
            {
                return new FixedRateValue( fixedRate.period(), fixedRate.timeUnit() );
            }
        }
        catch ( NoSuchMethodException | SecurityException e )
        {
            throw new RuntimeException( format( "failed to introspect class %s", scheduledProfiler.getClass() ), e );
        }
    }

    private Method findIntfMethod()
    {
        Class<ScheduledProfiler> intf = ScheduledProfiler.class;
        List<Method> intfMethods = Arrays.stream( intf.getMethods() )
                .filter( method -> method.getAnnotation( FixedRate.class ) != null )
                .collect( Collectors.toList() );

        if ( intfMethods.size() > 1 )
        {
            throw new RuntimeException( format( "%s has to many methods", intf ) );
        }
        return intfMethods.get( 0 );
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
