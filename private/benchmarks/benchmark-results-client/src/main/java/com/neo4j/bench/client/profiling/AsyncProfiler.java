package com.neo4j.bench.client.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.JvmVersion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.client.profiling.ProfilerType.ASYNC;
import static com.neo4j.bench.client.results.RunPhase.MEASUREMENT;
import static com.neo4j.bench.client.util.BenchmarkUtil.appendFile;
import static com.neo4j.bench.client.util.BenchmarkUtil.assertDirectoryExists;
import static com.neo4j.bench.client.util.BenchmarkUtil.assertDoesNotExist;
import static com.neo4j.bench.client.util.BenchmarkUtil.assertFileExists;

public class AsyncProfiler implements InternalProfiler, ExternalProfiler
{
    static final String ASYNC_PROFILER_DIR_ENV_VAR = "ASYNC_PROFILER_DIR";
    private static final String ASYNC_PROFILER_SCRIPT_NAME = "profiler.sh";
    private static final long DEFAULT_FRAME_BUFFER = 8 * 1024 * 1024;
    private static final String ASYNC_PROFILER_LOG = "async-profiler.log";
    private static final String ASYNC_LOG = "async.log";

    @Override
    public List<String> jvmInvokeArgs( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return Collections.emptyList();
    }

    @Override
    public List<String> jvmArgs( JvmVersion jvmVersion, ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return Lists.newArrayList(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+DebugNonSafepoints" );
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        // do nothing
    }

    @Override
    public void onWarmupBegin(
            Jvm jvm,
            ForkDirectory forkDirectory,
            long pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark )
    {
        // do nothing
    }

    @Override
    public void onWarmupFinished(
            Jvm jvm,
            ForkDirectory forkDirectory,
            long pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark )
    {
        // do nothing
    }

    @Override
    public void onMeasurementBegin(
            Jvm jvm,
            ForkDirectory forkDirectory,
            long pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark )
    {
        startAsync( forkDirectory, pid );
    }

    @Override
    public void onMeasurementFinished(
            Jvm jvm,
            ForkDirectory forkDirectory,
            long pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark )
    {
        stopAsync( forkDirectory, pid, new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, MEASUREMENT, ASYNC ) );
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        // do nothing
    }

    private void startAsync( ForkDirectory forkDirectory, long pid )
    {
        try
        {
            // profiler log -- used by this class only
            Path profilerLog = forkDirectory.create( ASYNC_PROFILER_LOG );

            // async profiler log -- used by async
            Path asyncLog = forkDirectory.create( ASYNC_LOG );

            // start profiling
            String[] asyncProfilerCommand = {
                    getAsyncProfilerScript().toAbsolutePath().toString(),
                    "start",
                    "-b", Long.toString( DEFAULT_FRAME_BUFFER ),
                    Long.toString( pid )};

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

    private void stopAsync( ForkDirectory forkDirectory, long pid, ProfilerRecordingDescriptor recordingDescriptor )
    {
        try
        {
            // async profiler recording
            Path asyncRecording = forkDirectory.pathFor( recordingDescriptor );
            assertDoesNotExist( asyncRecording );

            // profiler log -- used by this class only
            Path profilerLog = forkDirectory.findOrFail( ASYNC_PROFILER_LOG );

            // async profiler log -- used by async
            Path asyncLog = forkDirectory.findOrFail( ASYNC_LOG );

            // stop profiling
            String[] asyncProfilerCommand = {
                    getAsyncProfilerScript().toAbsolutePath().toString(),
                    "stop",
                    "-o", "collapsed",
                    "-b", Long.toString( DEFAULT_FRAME_BUFFER ),
                    "-f", asyncRecording.toAbsolutePath().toString(),
                    Long.toString( pid )};

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

            recordingDescriptor.profiler()
                               .maybeSecondaryRecordingCreator()
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
