/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.Benchmark.Mode;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.profiling.RecordingType;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.neo4j.bench.model.model.Benchmark.benchmarkFor;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OOMProfilerTest
{

    private static final Pattern HEAP_DUMP_FILE_PATTERN = Pattern.compile( "java_pid([0-9]+)\\.hprof" );

    private JvmVersion jvmVersion;
    private BenchmarkGroup benchmarkGroup;
    private Benchmark benchmark;
    private ForkDirectory forkDirectory;
    private Path forkDirectoryPath;

    @BeforeEach
    public void setUp( @TempDir Path path ) throws Exception
    {
        // given
        Jvm jvm = Jvm.defaultJvmOrFail();
        jvmVersion = jvm.version();

        benchmarkGroup = new BenchmarkGroup( "group" );
        BenchmarkGroupDirectory benchmarkGroupDirectory = BenchmarkGroupDirectory.findOrCreateAt( path, benchmarkGroup );

        benchmark = benchmarkFor(
                "description",
                "simpleName",
                Mode.THROUGHPUT,
                Collections.emptyMap() );
        BenchmarkDirectory benchmarkDirectory = benchmarkGroupDirectory.findOrCreate( benchmark );

        forkDirectory = benchmarkDirectory.findOrCreate( "fork" );
        forkDirectoryPath = Paths.get( forkDirectory.toAbsolutePath() );
    }

    @Test
    public void jvmArgsShouldContainHeapDumpArgs()
    {

        OOMProfiler oomProfiler = new OOMProfiler();

        // when
        JvmArgs jvmArgs = oomProfiler.jvmArgs( jvmVersion,
                                               forkDirectory,
                                               getProfilerRecordingDescriptor() );

        // then
        Path outMemoryScript = forkDirectoryPath.resolve( "on-out-of-memory.sh" );
        assertTrue( outMemoryScript.toFile().exists(), "Out memory scripts doesn't exist at " + outMemoryScript );
        assertThat( jvmArgs.toArgs(), contains(
                format( "-XX:OnOutOfMemoryError=%s --jvm-pid %%p --output-dir %s/out-of-memory", outMemoryScript, forkDirectory.toAbsolutePath() ),
                "-XX:+HeapDumpOnOutOfMemoryError",
                format( "-XX:HeapDumpPath=%s/out-of-memory", forkDirectory.toAbsolutePath() ) ) );
    }

    @Test
    public void copyHeapDumpWhenProcessFailed() throws Exception
    {

        OOMProfiler oomProfiler = new OOMProfiler();
        ProfilerRecordingDescriptor profilerRecordingDescriptor = getProfilerRecordingDescriptor();

        // when
        oomProfiler.jvmArgs( jvmVersion,
                             forkDirectory,
                             profilerRecordingDescriptor );
        // create mock heap dump
        Files.createFile( OOMProfiler.getOOMDirectory( forkDirectory ).resolve( "12345.hprof" ) );
        oomProfiler.processFailed( forkDirectory, profilerRecordingDescriptor );

        // then
        Path recordingPath = forkDirectory.findRegisteredPathFor( profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.HEAP_DUMP ) );

        assertTrue( Files.isRegularFile( recordingPath ) );
    }

    @Test
    public void copyEmptyHeapDumpWhenProcessSucceeded()
    {

        OOMProfiler oomProfiler = new OOMProfiler();
        ProfilerRecordingDescriptor profilerRecordingDescriptor = getProfilerRecordingDescriptor();

        // when
        oomProfiler.jvmArgs( jvmVersion,
                             forkDirectory,
                             profilerRecordingDescriptor );
        oomProfiler.afterProcess( forkDirectory, profilerRecordingDescriptor );

        // then
        Path recordingPath = forkDirectory.findRegisteredPathFor( profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.HEAP_DUMP ) );

        assertTrue( Files.isRegularFile( recordingPath ) );
    }

    @Test
    public void runProcessAndGenerateHeapDump() throws Exception
    {
        // given
        OOMProfiler oomProfiler = new OOMProfiler();

        String javaClassPath = System.getProperty( "java.class.path" );
        Path javaHome = Paths.get( System.getProperty( "java.home" ) );
        Path java = javaHome.resolve( "bin/java" );

        // when
        ProfilerRecordingDescriptor profilerRecordingDescriptor = getProfilerRecordingDescriptor();
        JvmArgs jvmArgs = oomProfiler.jvmArgs( jvmVersion,
                                               forkDirectory,
                                               profilerRecordingDescriptor );
        List<String> args = Lists.newArrayList(
                java.toString(),
                "-Xmx16m",
                "-cp",
                javaClassPath
        );
        args.addAll( jvmArgs.toArgs() );
        args.add( OutOfMemory.class.getName() );
        Process process = new ProcessBuilder()
                .command( args )
                .redirectErrorStream( true )
                .redirectOutput( ProcessBuilder.Redirect.INHERIT )
                .start();
        int waitFor = process.waitFor();
        assertNotEquals( 0, waitFor, "process should exit with non-zero code" );
        // then
        Path oomDirectory = OOMProfiler.getOOMDirectory( forkDirectory );
        // check if there is a single heap dump file
        File[] memoryDumps = oomDirectory.toFile().listFiles( (FileFilter) new RegexFileFilter( HEAP_DUMP_FILE_PATTERN ) );
        assertEquals( 1, memoryDumps.length );
        File memoryDump = memoryDumps[0];
        Matcher matcher = HEAP_DUMP_FILE_PATTERN.matcher( memoryDump.getName() );
        // check is additional dumps are there, like vmstat or pidstat
        if ( matcher.matches() )
        {
            String javaPid = matcher.group( 1 );
            assertTrue( Files.isRegularFile( oomDirectory.resolve( javaPid ).resolve( "pidstat.out" ) ) );
            assertTrue( Files.isRegularFile( oomDirectory.resolve( javaPid ).resolve( "vmstat.out" ) ) );
            assertTrue( Files.isRegularFile( oomDirectory.resolve( javaPid ).resolve( "processes.out" ) ) );
        }
        else
        {
            fail( format( "heap dump file name %s didn't match the pattern", memoryDump ) );
        }
    }

    private ProfilerRecordingDescriptor getProfilerRecordingDescriptor()
    {
        return ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                   benchmark,
                                                   RunPhase.MEASUREMENT,
                                                   ParameterizedProfiler.defaultProfiler( ProfilerType.OOM ),
                                                   Parameters.NONE );
    }
}
