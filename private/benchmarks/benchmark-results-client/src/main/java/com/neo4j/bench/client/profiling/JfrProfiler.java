/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.JvmVersion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.client.profiling.ProfilerType.JFR;
import static com.neo4j.bench.client.results.RunPhase.MEASUREMENT;
import static com.neo4j.bench.client.results.RunPhase.WARMUP;
import static com.neo4j.bench.client.util.BenchmarkUtil.appendFile;
import static com.neo4j.bench.client.util.BenchmarkUtil.assertDoesNotExist;

import static java.lang.String.format;

public class JfrProfiler implements InternalProfiler, ExternalProfiler
{
    private static final boolean DUMP_ON_EXIT = false;
    private static final String JFR_PROFILER_LOG = "jfr-profiler.log";
    private static final String JFR_LOG = "jfr.log";

    /*
        // start profiling
        jcmd <pid> JFR.start settings=profile

        // get recording id
        jcmd <pid> JFR.check

        // export recording
        jcmd <pid> JFR.dump recording=<recording id> filename=your-benchmark.jfr

        // stop profiler
        jcmd <pid> JFR.stop recording=<recording id>
    */

    @Override
    public List<String> jvmInvokeArgs( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return Collections.emptyList();
    }

    @Override
    public List<String> jvmArgs( JvmVersion jvmVersion, ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        ArrayList<String> argsTail = Lists.newArrayList(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+FlightRecorder",
                "-XX:+DebugNonSafepoints",
                "-XX:+PreserveFramePointer",
                "-XX:FlightRecorderOptions=stackdepth=256" );
        List<String> jvmArgs = Lists.newArrayList();

        if ( jvmVersion.majorVersion() < 11 &&
             jvmVersion.runtimeName().equals( "Java(TM) SE Runtime Environment" ) )
        {
            jvmArgs = Lists.newArrayList( "-XX:+UnlockCommercialFeatures" );
        }
        jvmArgs.addAll( argsTail );
        return jvmArgs;
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
        startJfr( jvm, forkDirectory, pid, new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, WARMUP, JFR ) );
    }

    @Override
    public void onWarmupFinished(
            Jvm jvm,
            ForkDirectory forkDirectory,
            long pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark )
    {
        stopJfr( jvm, forkDirectory, pid, new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, WARMUP, JFR ) );
    }

    @Override
    public void onMeasurementBegin(
            Jvm jvm,
            ForkDirectory forkDirectory,
            long pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark )
    {
        startJfr( jvm, forkDirectory, pid, new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, MEASUREMENT, JFR ) );
    }

    @Override
    public void onMeasurementFinished(
            Jvm jvm,
            ForkDirectory forkDirectory,
            long pid,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark )
    {
        stopJfr( jvm, forkDirectory, pid, new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, MEASUREMENT, JFR ) );
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        // do nothing
    }

    private void startJfr( Jvm jvm, ForkDirectory forkDirectory, long pid, ProfilerRecordingDescriptor recordingDescriptor )
    {
        try
        {
            // profiler log -- used by this class only
            Path profilerLog = forkDirectory.findOrCreate( JFR_PROFILER_LOG );

            // JFR profiler log -- used by JFR
            Path jfrLog = forkDirectory.findOrCreate( JFR_LOG );

            // -----------------------------------------------------------------------------------------------
            // ------------------------------------- start JFR profiler --------------------------------------
            // -----------------------------------------------------------------------------------------------
            // NOTE: sometimes interesting things occur after benchmark, e.g., db shutdown takes long time. dumponexit setting helps investigate those cases.
            String[] startJfrCommand = !DUMP_ON_EXIT ? new String[6] : new String[7];
            startJfrCommand[0] = jvm.launchJcmd();
            startJfrCommand[1] = Long.toString( pid );
            startJfrCommand[2] = "JFR.start";
            startJfrCommand[3] = "settings=profile";
            startJfrCommand[4] = "name=" + recordingDescriptor.name();
            startJfrCommand[5] = "dumponexit=" + DUMP_ON_EXIT;
            if ( DUMP_ON_EXIT )
            {
                startJfrCommand[6] = "filename='" + forkDirectory.pathFor( recordingDescriptor ).toAbsolutePath() + "'";
            }

            appendFile( profilerLog,
                        Instant.now(),
                        "Starting jfr profiler...",
                        "Command: " + String.join( " ", startJfrCommand ),
                        "-------------------------------" );

            Process startJfr = new ProcessBuilder( startJfrCommand )
                    .redirectOutput( jfrLog.toFile() )
                    .redirectError( jfrLog.toFile() )
                    .start();

            int resultCode = startJfr.waitFor();
            String jfrLogContents = new String( Files.readAllBytes( jfrLog ) );
            if ( resultCode != 0 || jfrLogContents.contains( "Could not start recording" ) )
            {
                appendFile( profilerLog,
                            Instant.now(),
                            "Bad things happened when starting JFR recording",
                            "See: " + jfrLog.toAbsolutePath(),
                            "-------------------------------" );
                throw new RuntimeException(
                        "Bad things happened when starting JFR recording, result code = " + resultCode + "\n" +
                        "See: " + jfrLog.toAbsolutePath() );
            }

            appendFile( profilerLog,
                        Instant.now(),
                        "JFR profiler started",
                        "-------------------------------" );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error trying to start JFR profiler", e );
        }
    }

    private void stopJfr( Jvm jvm, ForkDirectory forkDirectory, long pid, ProfilerRecordingDescriptor recordingDescriptor )
    {
        try
        {
            Path jfrProfilerOutput = forkDirectory.pathFor( recordingDescriptor );

            // profiler log -- used by this class only
            Path profilerLog = forkDirectory.findOrFail( JFR_PROFILER_LOG );

            // JFR profiler log -- used by JFR
            Path jfrLog = forkDirectory.findOrFail( JFR_LOG );

            assertDoesNotExist( jfrProfilerOutput );

            // -----------------------------------------------------------------------------------------------
            // ------------------------------- dump profiler recording to file -------------------------------
            // -----------------------------------------------------------------------------------------------

            String[] dumpJfrCommand = {
                    jvm.launchJcmd(),
                    Long.toString( pid ),
                    "JFR.dump",
                    format( "name=%s", recordingDescriptor.name() ),
                    format( "filename='%s'", jfrProfilerOutput.toAbsolutePath() )};

            appendFile( profilerLog,
                        Instant.now(),
                        "Dumping JFR recording...",
                        "Command: " + String.join( " ", dumpJfrCommand ),
                        "-------------------------------" );

            Process dumpJfr = new ProcessBuilder( dumpJfrCommand )
                    .redirectOutput( jfrLog.toFile() )
                    .redirectError( jfrLog.toFile() )
                    .start();
            int resultCode = dumpJfr.waitFor();
            if ( resultCode != 0 )
            {
                appendFile( profilerLog,
                            Instant.now(),
                            "Bad things happened when dropping JFR recording",
                            "See: " + jfrLog.toAbsolutePath(),
                            "-------------------------------" );
                throw new RuntimeException(
                        "Bad things happened when dropping JFR recording\n" +
                        "See: " + jfrLog.toAbsolutePath() );
            }

            if ( !Files.exists( jfrProfilerOutput ) )
            {
                throw new RuntimeException(
                        "A bad thing happened. No JFR profiler recording was created.\n" +
                        "Expected but did not find: " + jfrProfilerOutput.toAbsolutePath() );
            }

            // -----------------------------------------------------------------------------------------------
            // -------------------------------------- stop JFR profiler --------------------------------------
            // -----------------------------------------------------------------------------------------------

            String[] stopJfrCommand = {
                    "jcmd",
                    Long.toString( pid ),
                    "JFR.stop",
                    format( "name=%s", recordingDescriptor.name() )};

            appendFile( profilerLog,
                        Instant.now(),
                        "Stopping JFR profiler...",
                        "Command: " + String.join( " ", stopJfrCommand ),
                        "-------------------------------" );

            Process stopJfr = new ProcessBuilder( stopJfrCommand )
                    .redirectOutput( jfrLog.toFile() )
                    .redirectError( jfrLog.toFile() )
                    .start();
            resultCode = stopJfr.waitFor();
            if ( resultCode != 0 )
            {
                appendFile( profilerLog,
                            Instant.now(),
                            "Bad things happened when stopping JFR profiler",
                            "See: " + jfrLog.toAbsolutePath(),
                            "-------------------------------" );
                throw new RuntimeException(
                        "Bad things happened when stopping JFR profiler\n" +
                        "See: " + jfrLog.toAbsolutePath() );
            }

            appendFile( profilerLog,
                        Instant.now(),
                        "Profiling complete: " + jfrProfilerOutput.toAbsolutePath(),
                        "-------------------------------" );

            recordingDescriptor.profiler()
                               .maybeSecondaryRecordingCreator()
                               .ifPresent( secondaryRecordingCreator -> secondaryRecordingCreator.create( recordingDescriptor, forkDirectory ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error trying to stop JFR profiler", e );
        }
    }
}
