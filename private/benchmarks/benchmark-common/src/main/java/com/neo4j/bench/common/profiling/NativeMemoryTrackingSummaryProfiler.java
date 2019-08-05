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
import com.neo4j.bench.common.profiling.nmt.NativeMemoryTrackingSummaryReport;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;

import java.io.IOError;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class NativeMemoryTrackingSummaryProfiler implements ExternalProfiler, ScheduledProfiler
{

    public static final String SNAPSHOT_PARAM = "snapshot";

    // TODO: state, because we need to keep
    // snapshot number for ScheduledProfiler
    // I don't like it, we can modify it
    // by slight modification of ScheduledProfiler interface,
    // we can return state from onSchedule method and
    // pass state explicitly as method parameter
    private int snapshot;

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
        // TODO: I am not sure, how to handle this,
        // maybe this should be a secondary recording creator?
        // we take here all native memory tracking summaries and
        // aggregate them, we can than save them to CSV or JSON
        try
        {
            NativeMemoryTrackingSummaryReport summaryReport = NativeMemoryTrackingSummaryReport.create(
                    forkDirectory,
                    benchmarkGroup,
                    benchmark,
                    RunPhase.MEASUREMENT );
            summaryReport.toCSV( forkDirectory.create( "native_memory_tracking.summary.csv" ) );
        }
        catch ( IOException e )
        {
            throw new IOError( e );
        }
    }

    @Override
    public void onSchedule(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters,
            Pid pid )
    {
        Parameters parameters = incrementSnapshot( additionalParameters );
        ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create(
                benchmarkGroup,
                benchmark,
                RunPhase.MEASUREMENT,
                ProfilerType.NMT_SUMMARY,
                parameters );

        String recordingDescriptorFilename = recordingDescriptor.filename();
        List<String> command =
                asList( Jvm.defaultJvmOrFail().launchJcmd(), Long.toString( pid.get() ), "VM.native_memory", "summary", "scale=KB" );
        try
        {
            Process process = new ProcessBuilder( command )
                    .redirectOutput( forkDirectory.create( recordingDescriptorFilename ).toFile() ).start();
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

    private Parameters incrementSnapshot( Parameters additionalParameters )
    {
        Map<String,String> map = new HashMap<>( additionalParameters.asMap() );
        map.put( SNAPSHOT_PARAM, Integer.toString( snapshot++ ) );
        return new Parameters( map );
    }

}
