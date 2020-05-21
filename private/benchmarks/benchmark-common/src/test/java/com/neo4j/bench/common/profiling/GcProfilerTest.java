/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collections;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.model.model.Benchmark.Mode;
import static com.neo4j.bench.model.model.Benchmark.benchmarkFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

@TestDirectoryExtension
class GcProfilerTest
{
    private static final Logger LOG = LoggerFactory.getLogger( GcProfilerTest.class );

    @Inject
    private TestDirectory tempFolder;
    private ForkDirectory forkDirectory;
    private BenchmarkGroup benchmarkGroup;
    private Benchmark benchmark;

    @BeforeEach
    void setUp() throws Exception
    {

        Path parentDir = tempFolder.absolutePath().toPath();

        benchmarkGroup = new BenchmarkGroup( "group" );
        benchmark = benchmarkFor( "description", "simpleName", Mode.LATENCY, Collections.emptyMap() );

        BenchmarkGroupDirectory groupDirectory = BenchmarkGroupDirectory.createAt( parentDir, benchmarkGroup );
        BenchmarkDirectory benchmarkDirectory = groupDirectory.findOrCreate( benchmark );
        forkDirectory = benchmarkDirectory.create( "fork", Collections.emptyList() );
    }

    @Test
    void jvmArgsForJdk8()
    {
        GcProfiler profiler = new GcProfiler();
        JvmVersion jvmVersion = JvmVersion.create( 8, "" );
        JvmArgs jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.CLIENT, null );
        assertThat( jvmArgs.toArgs(), hasItem( equalTo( "-XX:+PrintGC" ) ) );
    }

    @Test
    void jvmArgsForJdk9()
    {
        GcProfiler profiler = new GcProfiler();
        JvmVersion jvmVersion = JvmVersion.create( 9, "" );
        JvmArgs jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.NONE, null );
        LOG.debug( jvmArgs.toString() );
        assertThat( jvmArgs.toArgs(), hasItem( startsWith( "-Xlog:gc,safepoint,gc+age=trace" ) ) );
    }
}
