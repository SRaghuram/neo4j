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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertThat;

public class GcProfilerTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private ForkDirectory forkDirectory;
    private BenchmarkGroup benchmarkGroup;
    private Benchmark benchmark;

    @Before
    public void setUp() throws Exception
    {

        Path parentDir = tempFolder.newFolder().toPath();

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
        JvmVersion jvmVersion = JvmVersion.create( 8, "Oracle Corporation" );
        List<String> jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark );
        assertThat( jvmArgs, hasItem( equalTo( "-XX:+PrintGC" ) ) );
    }

    @Test
    public void jvmArgsForJdk9() throws Exception
    {
        GcProfiler profiler = new GcProfiler();
        JvmVersion jvmVersion = JvmVersion.create( 9, "Oracle Corporation" );
        List<String> jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark );
        System.out.println( jvmArgs );
        assertThat( jvmArgs, hasItem( startsWith( "-Xlog:gc,safepoint,gc+age=trace" ) ) );
    }
}
