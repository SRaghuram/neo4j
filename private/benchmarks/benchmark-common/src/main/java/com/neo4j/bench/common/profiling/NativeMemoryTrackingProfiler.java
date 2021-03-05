/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.process.ProcessWrapper;
import com.neo4j.bench.common.profiling.nmt.NativeMemoryTrackingSnapshot;
import com.neo4j.bench.common.profiling.nmt.NativeMemoryTrackingSummaryReport;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.model.profiling.RecordingType;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class NativeMemoryTrackingProfiler implements ScheduledProfiler
{

    @Override
    public List<String> invokeArgs(
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        return emptyList();
    }

    @Override
    public JvmArgs jvmArgs(
            JvmVersion jvmVersion,
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        return JvmArgs.from( "-XX:NativeMemoryTracking=summary" );
    }

    @Override
    public void beforeProcess(
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
    }

    @Override
    public void afterProcess(
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.NMT_SUMMARY );
        try
        {
            NativeMemoryTrackingSummaryReport summaryReport =
                    NativeMemoryTrackingSummaryReport.create( Paths.get( forkDirectory.toAbsolutePath() ) );
            Path nmtRecording = forkDirectory.registerPathFor( recordingDescriptor );
            summaryReport.toCSV( nmtRecording );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Error creating NMT recording for:\n%s", recordingDescriptor ), e );
        }
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        // do nothing
    }

    @Override
    public void onSchedule(
            Tick tick,
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor,
            Jvm jvm,
            Pid pid )
    {
        File snapshotFile = forkDirectory.create( NativeMemoryTrackingSnapshot.snapshotFilename( tick.counter() ) ).toFile();
        List<String> command =
                asList( jvm.launchJcmd(), Long.toString( pid.get() ), "VM.native_memory", "summary", "scale=KB" );
        ProcessBuilder processBuilder = new ProcessBuilder( command ).redirectOutput( snapshotFile );

        ProcessWrapper processWrapper = ProcessWrapper.start( processBuilder );

        processWrapper.waitFor();
    }
}
