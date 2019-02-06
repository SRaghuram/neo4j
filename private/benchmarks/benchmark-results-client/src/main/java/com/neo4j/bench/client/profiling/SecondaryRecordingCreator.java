/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.client.process.ProcessWrapper;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.JsonUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

abstract class SecondaryRecordingCreator
{
    static final SecondaryRecordingCreator NONE = new None();
    private static final String FLAME_GRAPH_DIR = "FLAMEGRAPH_DIR";
    private static final String JFR_FLAMEGRAPH_DIR = "JFR_FLAMEGRAPH";

    abstract Set<String> requiredEnvironmentVariables();

    abstract Set<RecordingType> recordingTypes();

    abstract void create( ProfilerRecordingDescriptor recordingDescriptor, ForkDirectory forkDirectory );

    private static class None extends SecondaryRecordingCreator
    {

        @Override
        Set<String> requiredEnvironmentVariables()
        {
            return Collections.emptySet();
        }

        @Override
        Set<RecordingType> recordingTypes()
        {
            return Collections.emptySet();
        }

        @Override
        void create( ProfilerRecordingDescriptor recordingDescriptor, ForkDirectory forkDirectory )
        {
            // do nothing
        }
    }

    static class GcLogProcessor extends SecondaryRecordingCreator
    {
        @Override
        Set<String> requiredEnvironmentVariables()
        {
            return Collections.emptySet();
        }

        @Override
        Set<RecordingType> recordingTypes()
        {
            return Sets.newHashSet( RecordingType.GC_CSV, RecordingType.GC_SUMMARY );
        }

        @Override
        void create( ProfilerRecordingDescriptor recordingDescriptor, ForkDirectory forkDirectory )
        {
            Path gcLogFile = forkDirectory.pathFor( recordingDescriptor );
            try
            {
                GcLog gcLog = GcLog.parse( gcLogFile );

                Path gcLogJson = forkDirectory.pathFor( recordingDescriptor.filename( RecordingType.GC_SUMMARY ) );
                JsonUtil.serializeJson( gcLogJson, gcLog );

                Path gcLogCsv = forkDirectory.pathFor( recordingDescriptor.filename( RecordingType.GC_CSV ) );
                gcLog.toCSV( gcLogCsv );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Error processing GC log file: " + gcLogFile.toAbsolutePath(), e );
            }
        }
    }

    static class JfrFlameGraphCreator extends SecondaryRecordingCreator
    {
        private static final String CREATE_FLAMEGRAPH_SH = "create_flamegraph.sh";

        @Override
        Set<String> requiredEnvironmentVariables()
        {
            return Sets.newHashSet( SecondaryRecordingCreator.FLAME_GRAPH_DIR, SecondaryRecordingCreator.JFR_FLAMEGRAPH_DIR );
        }

        @Override
        Set<RecordingType> recordingTypes()
        {
            return Sets.newHashSet( RecordingType.JFR_FLAMEGRAPH );
        }

        @Override
        void create( ProfilerRecordingDescriptor recordingDescriptor, ForkDirectory forkDirectory )
        {
            Path profilerRecording = SecondaryRecordingCreator.getProfilerRecording( forkDirectory, recordingDescriptor );
            Path flameGraphSvg = SecondaryRecordingCreator.getFlameGraphSvg( forkDirectory, recordingDescriptor, RecordingType.JFR_FLAMEGRAPH );
            Path jfrFlameGraphDir = BenchmarkUtil.getPathEnvironmentVariable( JFR_FLAMEGRAPH_DIR );
            BenchmarkUtil.assertDirectoryExists( jfrFlameGraphDir );

            // this a fallback for configurations
            // using old version of jfr-flame-graphs
            if ( !Files.exists( jfrFlameGraphDir.resolve( CREATE_FLAMEGRAPH_SH ) ) )
            {
                jfrFlameGraphDir = jfrFlameGraphDir.resolve( "bin" );
            }

            List<String> args = Lists.newArrayList( "bash",
                                                    CREATE_FLAMEGRAPH_SH,
                                                    "-f",
                                                    profilerRecording.toAbsolutePath().toString(),
                                                    "-i" );
            SecondaryRecordingCreator.waitOnProcess( args, jfrFlameGraphDir, recordingDescriptor.profiler(), profilerRecording, flameGraphSvg );
        }
    }

    static class AsyncFlameGraphCreator extends SecondaryRecordingCreator
    {
        @Override
        Set<String> requiredEnvironmentVariables()
        {
            return Sets.newHashSet( SecondaryRecordingCreator.FLAME_GRAPH_DIR );
        }

        @Override
        Set<RecordingType> recordingTypes()
        {
            return Sets.newHashSet( RecordingType.ASYNC_FLAMEGRAPH );
        }

        @Override
        void create( ProfilerRecordingDescriptor recordingDescriptor, ForkDirectory forkDirectory )
        {
            Path profilerRecording = SecondaryRecordingCreator.getProfilerRecording( forkDirectory, recordingDescriptor );
            Path flameGraphSvg = SecondaryRecordingCreator.getFlameGraphSvg( forkDirectory, recordingDescriptor, RecordingType.ASYNC_FLAMEGRAPH );
            Path asyncFlameGraphDir = BenchmarkUtil.getPathEnvironmentVariable( FLAME_GRAPH_DIR );
            BenchmarkUtil.assertDirectoryExists( asyncFlameGraphDir );

            List<String> args = Lists.newArrayList( "perl",
                                                    "flamegraph.pl",
                                                    "--colors=java",
                                                    profilerRecording.toAbsolutePath().toString() );
            SecondaryRecordingCreator.waitOnProcess( args, asyncFlameGraphDir, recordingDescriptor.profiler(), profilerRecording, flameGraphSvg );
        }
    }

    private static Path getFlameGraphSvg( ForkDirectory forkDirectory, ProfilerRecordingDescriptor recordingDescriptor, RecordingType recordingType )
    {
        Path flameGraphSvg = forkDirectory.pathFor( recordingDescriptor.filename( recordingType ) );
        BenchmarkUtil.assertDoesNotExist( flameGraphSvg );
        return flameGraphSvg;
    }

    private static Path getProfilerRecording( ForkDirectory forkDirectory, ProfilerRecordingDescriptor recordingDescriptor )
    {
        Path profilerRecording = forkDirectory.pathFor( recordingDescriptor );
        BenchmarkUtil.assertFileExists( profilerRecording );
        return profilerRecording;
    }

    private static void waitOnProcess( List<String> args, Path workDirectory, ProfilerType profilerType, Path profilerRecording, Path flameGraphSvg )
    {
        try
        {
            ProcessBuilder processBuilder = new ProcessBuilder()
                    .command( args )
                    .redirectOutput( flameGraphSvg.toFile() )
                    .redirectError( flameGraphSvg.toFile() )
                    .directory( workDirectory.toFile() );
            ProcessWrapper.start( processBuilder )
                          .waitFor();
            BenchmarkUtil.assertFileExists( flameGraphSvg );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error creating " + profilerType + " FlameGraph\n" +
                                        "From recording : " + profilerRecording.toAbsolutePath() + "\n" +
                                        "To FlameGraph  : " + flameGraphSvg.toAbsolutePath(),
                                        e );
        }
    }
}
