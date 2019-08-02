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
import com.neo4j.bench.common.results.ForkDirectory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public boolean isRunning()
    {
        return !scheduledThreadPool.isTerminated();
    }

    private void scheduleProfiler( ScheduledProfiler scheduledProfiler, ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup, Benchmark benchmark, Parameters additionalParameters, Pid pid )
    {
        scheduledThreadPool.scheduleAtFixedRate( () -> scheduledProfiler.onSchedule( forkDirectory, benchmarkGroup,
                benchmark, additionalParameters, pid ), 0, 5, TimeUnit.SECONDS );
    }
}
