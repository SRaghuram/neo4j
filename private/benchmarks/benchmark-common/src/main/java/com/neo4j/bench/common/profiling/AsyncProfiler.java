/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.profiling.ProfilerType.ASYNC;
import static com.neo4j.bench.common.results.RunPhase.MEASUREMENT;
import static com.neo4j.bench.common.util.BenchmarkUtil.appendFile;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDirectoryExists;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDoesNotExist;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileExists;

public class AsyncProfiler implements InternalProfiler, ExternalProfiler
{
    static final String ASYNC_PROFILER_DIR_ENV_VAR = "ASYNC_PROFILER_DIR";
    private static final String ASYNC_PROFILER_SCRIPT_NAME = "profiler.sh";
    private static final long DEFAULT_FRAME_BUFFER = 8 * 1024 * 1024;

    // profiler log -- used by this class only
    private static String asyncProfilerLogName( Parameters parameters )
    {
        String additionalParametersString = parameters.isEmpty() ? "" : "-" + parameters.toString();
        return "async-profiler" + additionalParametersString + ".log";
    }

    // Async profiler log -- used as redirect for the process that starts the Async recording
    private static String asyncLogName( Parameters parameters )
    {
        String additionalParametersString = parameters.isEmpty() ? "" : "-" + parameters.toString();
        return "async" + additionalParametersString + ".log";
    }

    @Override
    public List<String> invokeArgs( ForkDirectory forkDirectory,
                                    BenchmarkGroup benchmarkGroup,
                                    Benchmark benchmark,
                                    Parameters additionalParameters )
    {
        return Collections.emptyList();
    }

    @Override
    public JvmArgs jvmArgs( JvmVersion jvmVersion,
                            ForkDirectory forkDirectory,
                            BenchmarkGroup benchmarkGroup,
                            Benchmark benchmark,
                            Parameters additionalParameters,
                            Resources resources )
    {
        return JvmArgs.from(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+DebugNonSafepoints" );
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory,
                               BenchmarkGroup benchmarkGroup,
                               Benchmark benchmark,
                               Parameters additionalParameters )
    {
        // do nothing
    }

    @Override
    public void onWarmupBegin(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
        // do nothing
    }

    @Override
    public void onWarmupFinished(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Parameters additionalParameters )
    {
        // do nothing
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
        startAsync( forkDirectory, pid, additionalParameters );
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
        stopAsync( forkDirectory,
                   pid,
                   ProfilerRecordingDescriptor.create( benchmarkGroup, benchmark, MEASUREMENT, ASYNC, additionalParameters ) );
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory,
                              BenchmarkGroup benchmarkGroup,
                              Benchmark benchmark,
                              Parameters additionalParameters )
    {
        // do nothing
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               BenchmarkGroup benchmarkGroup,
                               Benchmark benchmark,
                               Parameters additionalParameters )
    {
        // do nothing
    }

    private void startAsync( ForkDirectory forkDirectory,
                             Pid pid,
                             Parameters additionalParameters )
    {
        try
        {
            // profiler log -- used by this class only
            Path profilerLog = forkDirectory.create( asyncProfilerLogName( additionalParameters ) );

            // Async profiler log -- used as redirect for the process that starts the Async recording
            Path asyncLog = forkDirectory.create( asyncLogName( additionalParameters ) );

            // start profiling
            String[] asyncProfilerCommand = {
                    getAsyncProfilerScript().toAbsolutePath().toString(),
                    "start",
                    "-b", Long.toString( DEFAULT_FRAME_BUFFER ),
                    Long.toString( pid.get() )};

            appendFile( profilerLog,
                        Instant.now(),
                        "Starting async profiler...",
                        "Command: " + String.join( " ", asyncProfilerCommand ),
                        "-------------------------------" );

            Process startAsync = new ProcessBuilder( asyncProfilerCommand )
                    .redirectOutput( asyncLog.toFile() )
                    .redirectError( asyncLog.toFile() )
                    .start();

            int resultCode = startAsync.waitFor();
            if ( resultCode != 0 )
            {
                String asyncLogContents = new String( Files.readAllBytes( asyncLog ) );
                // everything seems to be fine when output is as below, regardless of result code
                if ( !asyncLogContents.contains( "Error reading response: Success\n" ) )
                {
                    throw new RuntimeException(
                            "Bad things happened when invoking async profiler, result code = " + resultCode + "\n" +
                            "------------ Async Profiler Output ------------\n" +
                            asyncLogContents +
                            "-----------------------------------------------" );
                }
            }

            appendFile( profilerLog,
                        Instant.now(),
                        "Async profiler started",
                        "-------------------------------" );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error trying to start async profiler", e );
        }
    }

    private void stopAsync( ForkDirectory forkDirectory,
                            Pid pid,
                            ProfilerRecordingDescriptor recordingDescriptor )
    {
        try
        {
            // async profiler recording
            Path asyncRecording = forkDirectory.pathFor( recordingDescriptor );
            assertDoesNotExist( asyncRecording );

            // profiler log -- used by this class only
            Path profilerLog = forkDirectory.findOrFail( asyncProfilerLogName( recordingDescriptor.additionalParams() ) );

            // Async profiler log -- used as redirect for the process that starts the Async recording
            Path asyncLog = forkDirectory.findOrFail( asyncLogName( recordingDescriptor.additionalParams() ) );

            // stop profiling
            String[] asyncProfilerCommand = {
                    getAsyncProfilerScript().toAbsolutePath().toString(),
                    "stop",
                    "-o", "collapsed",
                    "-b", Long.toString( DEFAULT_FRAME_BUFFER ),
                    "-f", asyncRecording.toAbsolutePath().toString(),
                    Long.toString( pid.get() )};

            appendFile( profilerLog,
                        Instant.now(),
                        "Stopping async profiler...",
                        "Command: " + String.join( " ", asyncProfilerCommand ),
                        "-------------------------------" );

            Process stopAsync = new ProcessBuilder( asyncProfilerCommand )
                    .redirectOutput( asyncLog.toFile() )
                    .redirectError( asyncLog.toFile() )
                    .start();
            int resultCode = stopAsync.waitFor();
            if ( resultCode != 0 )
            {
                String asyncLogContents = new String( Files.readAllBytes( asyncLog ) );
                // everything seems to be fine when output is as below, regardless of result code
                if ( !asyncLogContents.contains( "Error reading response: Success\n" ) )
                {
                    appendFile( profilerLog,
                                Instant.now(),
                                "Bad things happened when stopping async profiler",
                                "See: " + asyncLog.toAbsolutePath(),
                                "-------------------------------" );
                    throw new RuntimeException(
                            "Bad things happened when stopping async profiler\n" +
                            "See: " + profilerLog.toAbsolutePath() );
                }
            }

            if ( !Files.exists( asyncRecording ) )
            {
                throw new RuntimeException(
                        "A bad thing happened. No Async profiler recording was created.\n" +
                        "Expected but did not find: " + asyncRecording.toAbsolutePath() );
            }

            appendFile( profilerLog,
                        Instant.now(),
                        "Profiling complete: " + asyncRecording.toAbsolutePath(),
                        "-------------------------------" );
            String[] asyncOutputSyncCommand = {"sync", asyncRecording.toAbsolutePath().toString()};
            Process syncAsync = new ProcessBuilder( asyncOutputSyncCommand )
                    .redirectOutput( profilerLog.toFile() )
                    .redirectError( profilerLog.toFile() )
                    .start();

            resultCode = syncAsync.waitFor();
            if ( resultCode != 0 )
            {
                appendFile( profilerLog,
                            Instant.now(),
                            "Bad things happened when syncing Async file",
                            "See: " + profilerLog.toAbsolutePath(),
                            "-------------------------------" );
                throw new RuntimeException(
                        "Bad things happened when syncing Async file\n" +
                        "See: " + profilerLog.toAbsolutePath() );
            }
            ASYNC.maybeSecondaryRecordingCreator()
                 .ifPresent( secondaryRecordingCreator -> secondaryRecordingCreator.create( recordingDescriptor, forkDirectory ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error trying to stop async profiler", e );
        }
    }

    private static Path getAsyncProfilerScript()
    {
        Path asyncProfilerDir = BenchmarkUtil.getPathEnvironmentVariable( ASYNC_PROFILER_DIR_ENV_VAR );
        assertDirectoryExists( asyncProfilerDir );
        Path asyncScript = asyncProfilerDir.resolve( ASYNC_PROFILER_SCRIPT_NAME );
        assertFileExists( asyncScript );
        return asyncScript;
    }
}
