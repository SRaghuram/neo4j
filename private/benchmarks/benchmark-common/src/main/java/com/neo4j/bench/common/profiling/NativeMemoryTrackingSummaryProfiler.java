/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.Maps;
import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.nmt.NativeMemoryTrackingSummaryReport;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class NativeMemoryTrackingSummaryProfiler implements ExternalProfiler, InternalProfiler
{

    public static final String SNAPSHOT_PARAM = "snapshot";

    private ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool( 1 );

    @Override
    public List<String> invokeArgs(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
        return emptyList();
    }

    @Override
    public List<String> jvmArgs(
            JvmVersion jvmVersion,
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
        return asList( "-XX:NativeMemoryTracking=summary" );
    }

    @Override
    public void beforeProcess(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
    }

    @Override
    public void afterProcess(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
    }

    @Override
    public void onWarmupBegin( Jvm jvm, ForkDirectory forkDirectory, Pid pid, BenchmarkGroup benchmarkGroup,
            Benchmark benchmark, Parameters additionalParameters )
    {
    }

    @Override
    public void onWarmupFinished( Jvm jvm, ForkDirectory forkDirectory, Pid pid, BenchmarkGroup benchmarkGroup,
            Benchmark benchmark, Parameters additionalParameters )
    {
    }

    @Override
    public void onMeasurementBegin(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {

        Map<String,String> map = additionalParameters.asMap();
        HashMap<String,String> newMap = Maps.newHashMap(map);
        newMap.put( SNAPSHOT_PARAM, "0" );

        scheduledThreadPool.schedule(
                snapshotNativeMemorySummary( jvm, forkDirectory, pid, benchmarkGroup, benchmark, new Parameters( newMap ) ),
                5, TimeUnit.SECONDS );
    }

    @Override
    public void onMeasurementFinished(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
        scheduledThreadPool.shutdown();
        try
        {
            scheduledThreadPool.awaitTermination( 10, TimeUnit.SECONDS );
        }
        catch ( InterruptedException e )
        {
            System.err.println( format( "failed to terminate scheduler\n%s ", e ) );
        }

        try
        {
            NativeMemoryTrackingSummaryReport summaryReport = NativeMemoryTrackingSummaryReport.create(
                    forkDirectory,
                    benchmarkGroup,
                    benchmark,
                    RunPhase.MEASUREMENT );
            summaryReport.toCSV( forkDirectory.findOrCreate( "ntm.summary.report.csv" ) );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }

    }

    private Runnable snapshotNativeMemorySummary(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {

        return () -> {
            // first, capture snapshot of native memory tracking summary
            captureNativeMemorySummary( jvm, forkDirectory, pid, benchmarkGroup, benchmark, additionalParameters );
            // schedule next capture
            scheduleNextCapture( jvm, forkDirectory, pid, benchmarkGroup, benchmark, additionalParameters );
        };
    }

    private void captureNativeMemorySummary(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {

        // assert if we have snapshot property, which is number, if not fail
        assertSnapshotNumber( additionalParameters );

        ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create(
                benchmarkGroup,
                benchmark,
                RunPhase.MEASUREMENT,
                ProfilerType.NMT_SUMMARY,
                additionalParameters );

        String recordingDescriptorFilename = recordingDescriptor.filename();

        List<String> command = asList(jvm.launchJcmd(),Long.toString( pid.get() ), "VM.native_memory", "summary", "scale=KB");

        try
        {
            Process process = new ProcessBuilder( command )
                .redirectOutput( forkDirectory.create( recordingDescriptorFilename ).toFile() )
                .start();

            boolean waitFor = process.waitFor( 10, TimeUnit.SECONDS );
            if ( waitFor )
            {
                if ( process.exitValue() != 0 )
                {
                    System.out.println( format( "jcmd exited with non-zero (%d) exit code", process.exitValue() ) );
                }
            }
            else
            {
                process.destroyForcibly();
            }
        }
        catch ( IOException | InterruptedException e )
        {
            System.out.println( format( "failed to snapshot native memory tracking summary\n%s", e ) );
        }
    }

    private void assertSnapshotNumber( Parameters additionalParameters )
    {
        Map<String,String> map = additionalParameters.asMap();
        String snapshot = map.getOrDefault( SNAPSHOT_PARAM, "none" );
        try
        {
            Long.parseLong( snapshot );
        }
        catch ( NumberFormatException e )
        {
            throw new IllegalArgumentException( format( "native memory snapshot is not a number, it is %s", snapshot ) );
        }
    }

    private void scheduleNextCapture(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
        if ( !scheduledThreadPool.isShutdown() )
        {
            assertSnapshotNumber( additionalParameters );
            HashMap<String,String> newMap = incrementSnapshotNumber( additionalParameters );
            scheduledThreadPool.schedule( snapshotNativeMemorySummary(
                    jvm,
                    forkDirectory,
                    pid,
                    benchmarkGroup,
                    benchmark,
                    new Parameters( newMap ) ), 5, TimeUnit.SECONDS );
        }
        else
        {
            System.out.println( format( "will not capture next snapshot for pid %s", pid.get() ) );
        }
    }

    private static HashMap<String,String> incrementSnapshotNumber( Parameters additionalParameters )
    {
        long nextSnapshot = Long.parseLong( additionalParameters.asMap().get( SNAPSHOT_PARAM ) ) + 1;
        HashMap<String,String> newMap = Maps.newHashMap( additionalParameters.asMap() );
        newMap.put( SNAPSHOT_PARAM, Long.toString( nextSnapshot ) );
        return newMap;
    }

}
