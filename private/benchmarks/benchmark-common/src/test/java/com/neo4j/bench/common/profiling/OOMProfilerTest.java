/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.Benchmark.Mode;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;

public class OOMProfilerTest
{

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

        benchmark = Benchmark.benchmarkFor(
                "description",
                "simpleName",
                Mode.THROUGHPUT,
                Collections.emptyMap() );
        BenchmarkDirectory benchmarkDirectory = benchmarkGroupDirectory.findOrCreate( benchmark );

        forkDirectory = benchmarkDirectory.findOrCreate( "fork", singletonList( ProfilerType.OOM ) );
        forkDirectoryPath = Paths.get( forkDirectory.toAbsolutePath() );
    }

    @Test
    public void jvmArgsShouldContainHeapDumpArgs() throws Exception
    {

        OOMProfiler oomProfiler = new OOMProfiler();

        // when
        JvmArgs jvmArgs = null;
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
    public void copyEmptyHeapDumpWhenProcessSucceeded() throws Exception
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
}
