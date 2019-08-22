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
import com.neo4j.bench.common.process.ProcessWrapper;
import com.neo4j.bench.common.profiling.nmt.NativeMemoryTrackingSnapshot;
import com.neo4j.bench.common.profiling.nmt.NativeMemoryTrackingSummaryReport;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class NativeMemoryTrackingProfiler implements ScheduledProfiler
{

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
        Path csvReport = Paths.get( ProfilerRecordingDescriptor.create(
                benchmarkGroup,
                benchmark,
                RunPhase.MEASUREMENT,
                ProfilerType.NMT,
                additionalParameters ).sanitizedFilename() );
        try
        {
            NativeMemoryTrackingSummaryReport summaryReport =
                    NativeMemoryTrackingSummaryReport.create( Paths.get( forkDirectory.toAbsolutePath() ) );
            summaryReport.toCSV( csvReport );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "cannot create NMT profiler report at %s", csvReport ), e );
        }
    }

    @Override
    public void onSchedule(
            Tick tick,
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters,
            Jvm jvm,
            Pid pid )
    {
        String snapshotFilename = NativeMemoryTrackingSnapshot.snapshotFilename( tick.counter() );
        List<String> command =
                asList( jvm.launchJcmd(), Long.toString( pid.get() ), "VM.native_memory", "summary", "scale=KB" );
        ProcessBuilder processBuilder = new ProcessBuilder( command )
                .redirectOutput( forkDirectory.create( snapshotFilename ).toFile() );

        ProcessWrapper processWrapper = ProcessWrapper.start( processBuilder );

        processWrapper.waitFor();
    }

}
