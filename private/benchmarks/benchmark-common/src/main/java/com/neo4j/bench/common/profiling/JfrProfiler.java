/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.PathUtil;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.profiling.RecordingType;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.profiling.ProfilerType.JFR;
import static com.neo4j.bench.common.util.BenchmarkUtil.appendFile;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDoesNotExist;
import static java.lang.String.format;

public class JfrProfiler implements InternalProfiler, ExternalProfiler
{
    private static final boolean DUMP_ON_EXIT = false;

    // profiler log -- used by this class only
    private static String jfrProfilerLogName( Parameters parameters )
    {
        String additionalParametersString = parameters.isEmpty() ? "" : "-" + parameters.toString();
        return PathUtil.withDefaultMaxLength().limitLength( "jfr-profiler", additionalParametersString, ".log" );
    }

    // JFR profiler log -- used as redirect for the process that starts the JFR recording
    private static String jfrLogName( Parameters parameters )
    {
        String additionalParametersString = parameters.isEmpty() ? "" : "-" + parameters.toString();
        return PathUtil.withDefaultMaxLength().limitLength( "jfr", additionalParametersString, ".log" );
    }

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
    public List<String> invokeArgs( ForkDirectory forkDirectory,
                                    ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        return Collections.emptyList();
    }

    @Override
    public JvmArgs jvmArgs( JvmVersion jvmVersion,
                            ForkDirectory forkDirectory,
                            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        JvmArgs jvmArgs = JvmArgs.empty();
        if ( jvmVersion.hasCommercialFeatures() )
        {
            jvmArgs = jvmArgs.set( "-XX:+UnlockCommercialFeatures" );
        }
        jvmArgs = jvmArgs.addAll( Lists.newArrayList(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+FlightRecorder",
                "-XX:+DebugNonSafepoints",
                "-XX:+PreserveFramePointer",
                "-XX:FlightRecorderOptions=stackdepth=256" ) );
        return jvmArgs;
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        // do nothing
    }

    @Override
    public void onWarmupBegin(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        startJfr( jvm,
                  forkDirectory,
                  pid,
                  profilerRecordingDescriptor );
    }

    @Override
    public void onWarmupFinished(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        stopJfr( jvm,
                 forkDirectory,
                 pid,
                 profilerRecordingDescriptor );
    }

    @Override
    public void onMeasurementBegin(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        startJfr( jvm,
                  forkDirectory,
                  pid,
                  profilerRecordingDescriptor );
    }

    @Override
    public void onMeasurementFinished(
            Jvm jvm,
            ForkDirectory forkDirectory,
            Pid pid,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        stopJfr( jvm,
                 forkDirectory,
                 pid,
                 profilerRecordingDescriptor );
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory,
                              ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        // do nothing
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        // do nothing
    }

    private void startJfr( Jvm jvm,
                           ForkDirectory forkDirectory,
                           Pid pid,
                           ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        try
        {
            RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.JFR );

            // profiler log -- used by this class only
            Path profilerLog = forkDirectory.findOrCreate( jfrProfilerLogName( recordingDescriptor.additionalParams() ) );

            // JFR profiler log -- used as redirect for the process that starts the JFR recording
            Path jfrLog = forkDirectory.findOrCreate( jfrLogName( recordingDescriptor.additionalParams() ) );

            // -----------------------------------------------------------------------------------------------
            // ------------------------------------- start JFR profiler --------------------------------------
            // -----------------------------------------------------------------------------------------------
            // NOTE: sometimes interesting things occur after benchmark, e.g., db shutdown takes long time. dumponexit setting helps investigate those cases.
            String[] startJfrCommand = !DUMP_ON_EXIT ? new String[6] : new String[7];
            startJfrCommand[0] = jvm.launchJcmd();
            startJfrCommand[1] = Long.toString( pid.get() );
            startJfrCommand[2] = "JFR.start";
            startJfrCommand[3] = "settings=profile";
            startJfrCommand[4] = createNameArgument( recordingDescriptor );
            startJfrCommand[5] = "dumponexit=" + DUMP_ON_EXIT;
            if ( DUMP_ON_EXIT )
            {
                startJfrCommand[6] = "filename='" + forkDirectory.registerPathFor( recordingDescriptor ).toAbsolutePath() + "'";
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
            String jfrLogContents = Files.readString( jfrLog );
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

    private void stopJfr( Jvm jvm,
                          ForkDirectory forkDirectory,
                          Pid pid,
                          ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        try
        {
            RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.JFR );

            Path jfrProfilerOutput = forkDirectory.registerPathFor( recordingDescriptor );

            // profiler log -- used by this class only
            Path profilerLog = forkDirectory.findOrFail( jfrProfilerLogName( recordingDescriptor.additionalParams() ) );

            // JFR profiler log -- used as redirect for the process that starts the JFR recording
            Path jfrLog = forkDirectory.findOrFail( jfrLogName( recordingDescriptor.additionalParams() ) );

            assertDoesNotExist( jfrProfilerOutput );

            // -----------------------------------------------------------------------------------------------
            // ------------------------------- dump profiler recording to file -------------------------------
            // -----------------------------------------------------------------------------------------------

            String[] dumpJfrCommand = {
                    jvm.launchJcmd(),
                    Long.toString( pid.get() ),
                    "JFR.dump",
                    createNameArgument( recordingDescriptor ),
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
                    jvm.launchJcmd(),
                    Long.toString( pid.get() ),
                    "JFR.stop",
                    createNameArgument( recordingDescriptor )};

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
            String[] jfrOutputSyncCommand = {"sync", jfrProfilerOutput.toAbsolutePath().toString()};
            Process syncJfr = new ProcessBuilder( jfrOutputSyncCommand )
                    .redirectOutput( profilerLog.toFile() )
                    .redirectError( profilerLog.toFile() )
                    .start();

            resultCode = syncJfr.waitFor();
            if ( resultCode != 0 )
            {
                appendFile( profilerLog,
                            Instant.now(),
                            "Bad things happened when syncing JFR file",
                            "See: " + profilerLog.toAbsolutePath(),
                            "-------------------------------" );
                throw new RuntimeException(
                        "Bad things happened when syncing JFR file\n" +
                        "See: " + profilerLog.toAbsolutePath() );
            }
            JFR.maybeSecondaryRecordingCreator()
               .ifPresent( secondaryRecordingCreator -> secondaryRecordingCreator.create( profilerRecordingDescriptor, forkDirectory ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error trying to stop JFR profiler", e );
        }
    }

    private String createNameArgument( RecordingDescriptor recordingDescriptor )
    {
        return format( "name=%s",  recordingDescriptor.sanitizedName() );
    }
}
