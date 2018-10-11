/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.FakeClock;

class CapturingJobScheduler implements JobScheduler
{
    private final FakeClock clock;
    private final long skew;
    private final TimeUnit skewUnit;

    private Group group;
    private Runnable capturedRunnable;
    private long period;
    private TimeUnit timeUnit;

    CapturingJobScheduler( FakeClock clock )
    {
        this( clock, 0, TimeUnit.SECONDS );
    }

    CapturingJobScheduler( FakeClock clock, long skew, TimeUnit skewUnit )
    {
        this.clock = clock;
        this.skew = skew;
        this.skewUnit = skewUnit;
    }

    Group getGroup()
    {
        return group;
    }

    Runnable getCapturedRunnable()
    {
        return capturedRunnable;
    }

    Runnable getCapturedRunnableChangingClock()
    {
        return () ->
        {
            clock.forward( period, timeUnit );
            capturedRunnable.run();
        };
    }

    Runnable getCapturedRunnableWithSkew()
    {
        return () ->
        {
            clock.forward( skew, skewUnit );
            getCapturedRunnableChangingClock().run();
        };
    }

    long getPeriod()
    {
        return period;
    }

    TimeUnit getTimeUnit()
    {
        return timeUnit;
    }

    @Override
    public void setTopLevelGroupName( String name )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public Executor executor( Group group )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public ExecutorService workStealingExecutor( Group group, int parallelism )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public ExecutorService workStealingExecutorAsyncMode( Group group, int parallelism )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public JobHandle schedule( Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit )
    {
        this.group = group;
        this.capturedRunnable = runnable;
        this.period = period;
        this.timeUnit = timeUnit;
        return null;
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public void close() throws Exception
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public void init() throws Throwable
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public void start() throws Throwable
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public void stop() throws Throwable
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public void shutdown() throws Throwable
    {
        throw new RuntimeException( "Unimplemented" );
    }
}
