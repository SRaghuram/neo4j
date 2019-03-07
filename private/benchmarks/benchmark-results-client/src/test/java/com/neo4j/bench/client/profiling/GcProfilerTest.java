/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.Benchmark.Mode;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.results.BenchmarkDirectory;
import com.neo4j.bench.client.results.BenchmarkGroupDirectory;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.JvmVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

public class GcProfilerTest
{
    @TempDir
    public Path tempFolder;

    private ForkDirectory forkDirectory;
    private BenchmarkGroup benchmarkGroup;
    private Benchmark benchmark;

    @BeforeEach
    public void setUp() throws Exception
    {

        Path parentDir = Files.createTempDirectory( tempFolder, "" );

        benchmarkGroup = new BenchmarkGroup( "group" );
        benchmark = Benchmark.benchmarkFor( "description", "simpleName", Mode.LATENCY, Collections.emptyMap() );

        BenchmarkGroupDirectory groupDirectory = BenchmarkGroupDirectory.createAt( parentDir, benchmarkGroup );
        BenchmarkDirectory benchmarkDirectory = groupDirectory.findOrCreate( benchmark );
        forkDirectory = benchmarkDirectory.create( "fork", Collections.emptyList() );
    }

    @Test
    public void jvmArgsForJdk8() throws Exception
    {
        GcProfiler profiler = new GcProfiler();
        JvmVersion jvmVersion = JvmVersion.create( 8, "" );
        List<String> jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark );
        assertThat( jvmArgs, hasItem( equalTo( "-XX:+PrintGC" ) ) );
    }

    @Test
    public void jvmArgsForJdk9() throws Exception
    {
        GcProfiler profiler = new GcProfiler();
        JvmVersion jvmVersion = JvmVersion.create( 9, "" );
        List<String> jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark );
        System.out.println( jvmArgs );
        assertThat( jvmArgs, hasItem( startsWith( "-Xlog:gc,safepoint,gc+age=trace" ) ) );
    }
}
