/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.ScheduledProfiler;
import com.neo4j.bench.common.profiling.ScheduledProfiler.FixedRate;
import com.neo4j.bench.common.results.ForkDirectory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class ScheduledProfilers
{
    public static ScheduledProfilers from( List<ExternalProfiler> externalProfilers )
    {
        List<ScheduledProfiler> schedulerProfilers =
                externalProfilers.stream().filter( k -> k instanceof ScheduledProfiler )
                        .map( ScheduledProfiler.class::cast ).collect( Collectors.toList() );
        return new ScheduledProfilers( schedulerProfilers );
    }

    private final List<ScheduledProfiler> scheduledProfilers;
    private final ScheduledExecutorService scheduledThreadPool;

    public ScheduledProfilers( List<ScheduledProfiler> scheduledProfilers )
    {
        this.scheduledProfilers = scheduledProfilers;
        scheduledThreadPool = Executors.newScheduledThreadPool( Runtime.getRuntime().availableProcessors() );
    }

    public void start( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark,
            Parameters clientParameters, Pid pid )
    {
        scheduledProfilers.forEach( scheduledProfiler -> scheduleProfiler( scheduledProfiler, forkDirectory,
                benchmarkGroup, benchmark, clientParameters, pid ) );
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

    private void scheduleProfiler( ScheduledProfiler scheduledProfiler, ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup, Benchmark benchmark, Parameters additionalParameters, Pid pid )
    {
        FixedRateValue fixedRate = getFixedRate( scheduledProfiler );
        scheduledThreadPool.scheduleAtFixedRate(
                () -> scheduledProfiler.onSchedule( forkDirectory, benchmarkGroup, benchmark, additionalParameters,
                        pid ),
                0,
                fixedRate.period,
                fixedRate.timeUnit);
    }

    private FixedRateValue getFixedRate( ScheduledProfiler scheduledProfiler )
    {
        // find interface method
        Method intfMethod = findIntfMethod();
        try
        {
            Method method =
                    scheduledProfiler.getClass().getMethod( intfMethod.getName(), intfMethod.getParameterTypes() );
            FixedRate fixedRate = method.getAnnotation( ScheduledProfiler.FixedRate.class );
            if ( fixedRate == null )
            {
                return new FixedRateValue( 5, TimeUnit.SECONDS );
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
        Method[] intfMethods = intf.getMethods();
        if ( intfMethods.length > 1 )
        {
            throw new RuntimeException( format( "%s has to many methods", intf ) );
        }
        Method intfMethod = intfMethods[0];
        return intfMethod;
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
}
