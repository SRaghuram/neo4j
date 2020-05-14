/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.neo4j.bench.model.model.Benchmark.Mode;
import static com.neo4j.bench.model.model.Benchmark.benchmarkFor;
import static java.lang.String.format;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OOMProfilerTest
{

    private static final Pattern HEAP_DUMP_FILE_PATTERN = Pattern.compile( "java_pid([0-9]+)\\.hprof" );
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private JvmVersion jvmVersion;
    private BenchmarkGroup benchmarkGroup;
    private Benchmark benchmark;
    private ForkDirectory forkDirectory;
    private Path forkDirectoryPath;

    @Before
    public void setUp() throws Exception
    {
        // given
        Jvm jvm = Jvm.defaultJvmOrFail();
        jvmVersion = jvm.version();

        benchmarkGroup = new BenchmarkGroup( "group" );
        BenchmarkGroupDirectory benchmarkGroupDirectory = BenchmarkGroupDirectory.findOrCreateAt( temporaryFolder.newFolder().toPath(), benchmarkGroup );

        benchmark = benchmarkFor(
                "description",
                "simpleName",
                Mode.THROUGHPUT,
                Collections.emptyMap() );
        BenchmarkDirectory benchmarkDirectory = benchmarkGroupDirectory.findOrCreate( benchmark );

        forkDirectory = benchmarkDirectory.findOrCreate( "fork", ParameterizedProfiler.defaultProfilers( ProfilerType.OOM ) );
        forkDirectoryPath = Paths.get( forkDirectory.toAbsolutePath() );
    }

    @Test
    public void jvmArgsShouldContainHeapDumpArgs()
    {

        OOMProfiler oomProfiler = new OOMProfiler();

        // when
        JvmArgs jvmArgs;
        try ( Resources resources = new Resources( forkDirectoryPath ) )
        {
            jvmArgs = oomProfiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.NONE, resources );
        }

        // then
        assertThat( jvmArgs.toArgs(), contains(
                format( "-XX:OnOutOfMemoryError=%s --jvm-pid %%p --output-dir %s/out-of-memory",
                        forkDirectoryPath.resolve( "resources_copy/bench/profiling/on-out-of-memory.sh" ),
                        forkDirectory.toAbsolutePath() ),
                "-XX:+HeapDumpOnOutOfMemoryError",
                format( "-XX:HeapDumpPath=%s/out-of-memory", forkDirectory.toAbsolutePath() ) ) );
    }

    @Test
    public void copyHeapDumpWhenProcessFailed() throws Exception
    {

        OOMProfiler oomProfiler = new OOMProfiler();

        // when
        try ( Resources resources = new Resources( forkDirectoryPath ) )
        {
            oomProfiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.NONE, resources );
            // create mock heap dump
            Files.createFile( OOMProfiler.getOOMDirectory( forkDirectory ).resolve( "12345.hprof" ) );
            oomProfiler.processFailed( forkDirectory, benchmarkGroup, benchmark, Parameters.NONE );
        }

        // then
        ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create(
                benchmarkGroup,
                benchmark,
                RunPhase.MEASUREMENT,
                ProfilerType.OOM,
                Parameters.NONE );
        Path recordingPath = forkDirectory.pathFor( recordingDescriptor );

        assertTrue( Files.isRegularFile( recordingPath ) );
    }

    @Test
    public void copyEmptyHeapDumpWhenProcessSucceeded()
    {

        OOMProfiler oomProfiler = new OOMProfiler();

        // when
        try ( Resources resources = new Resources( forkDirectoryPath ) )
        {
            oomProfiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.NONE, resources );
            oomProfiler.afterProcess( forkDirectory, benchmarkGroup, benchmark, Parameters.NONE );
        }

        // then
        ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create(
                benchmarkGroup,
                benchmark,
                RunPhase.MEASUREMENT,
                ProfilerType.OOM,
                Parameters.NONE );
        Path recordingPath = forkDirectory.pathFor( recordingDescriptor );

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
        try ( Resources resources = new Resources( forkDirectoryPath ) )
        {
            JvmArgs jvmArgs = oomProfiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.NONE, resources );
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
            assertNotEquals( "process should exit with non-zero code", 0, waitFor );
        }
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
}
