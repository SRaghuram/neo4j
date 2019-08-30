/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.common.model.Benchmark.Mode;
import static com.neo4j.bench.common.util.TestDirectorySupport.createTempDirectoryPath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

@TestDirectoryExtension
class GcProfilerTest
{
    @Inject
    private TestDirectory tempFolder;

    private ForkDirectory forkDirectory;
    private BenchmarkGroup benchmarkGroup;
    private Benchmark benchmark;

    @BeforeEach
    void setUp() throws Exception
    {

        Path parentDir = createTempDirectoryPath( tempFolder.absolutePath() );

        benchmarkGroup = new BenchmarkGroup( "group" );
        benchmark = Benchmark.benchmarkFor( "description", "simpleName", Mode.LATENCY, Collections.emptyMap() );

        BenchmarkGroupDirectory groupDirectory = BenchmarkGroupDirectory.createAt( parentDir, benchmarkGroup );
        BenchmarkDirectory benchmarkDirectory = groupDirectory.findOrCreate( benchmark );
        forkDirectory = benchmarkDirectory.create( "fork", Collections.emptyList() );
    }

    @Test
    void jvmArgsForJdk8()
    {
        GcProfiler profiler = new GcProfiler();
        JvmVersion jvmVersion = JvmVersion.create( 8, "" );
        List<String> jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.CLIENT );
        assertThat( jvmArgs, hasItem( equalTo( "-XX:+PrintGC" ) ) );
    }

    @Test
    void jvmArgsForJdk9()
    {
        GcProfiler profiler = new GcProfiler();
        JvmVersion jvmVersion = JvmVersion.create( 9, "" );
        List<String> jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.NONE );
        System.out.println( jvmArgs );
        assertThat( jvmArgs, hasItem( startsWith( "-Xlog:gc,safepoint,gc+age=trace" ) ) );
    }
}
