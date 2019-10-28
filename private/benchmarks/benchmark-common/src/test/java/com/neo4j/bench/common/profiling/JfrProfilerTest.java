/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.TestDirectorySupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Collections;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.common.model.Benchmark.Mode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

@TestDirectoryExtension
class JfrProfilerTest
{
    @Inject
    private TestDirectory tempFolder;

    private ForkDirectory forkDirectory;
    private BenchmarkGroup benchmarkGroup;
    private Benchmark benchmark;

    @BeforeEach
    void setUp() throws Exception
    {
        Path parentDir = TestDirectorySupport.createTempDirectoryPath( tempFolder.absolutePath() );

        benchmarkGroup = new BenchmarkGroup( "group" );
        benchmark = Benchmark.benchmarkFor( "description", "simpleName", Mode.LATENCY, Collections.emptyMap() );

        BenchmarkGroupDirectory groupDirectory = BenchmarkGroupDirectory.createAt( parentDir, benchmarkGroup );
        BenchmarkDirectory benchmarkDirectory = groupDirectory.findOrCreate( benchmark );
        forkDirectory = benchmarkDirectory.create( "fork", Collections.emptyList() );
    }

    @Test
    void jvmArgsForPreJdk11()
    {
        JvmVersion jvmVersion = JvmVersion.create( 8, JvmVersion.JAVA_TM_SE_RUNTIME_ENVIRONMENT );
        JfrProfiler profiler = new JfrProfiler();
        JvmArgs jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.SERVER, null );
        assertThat( jvmArgs.toArgs(), hasItem( equalTo( "-XX:+UnlockCommercialFeatures" ) ) );
    }

    @Test
    void jvmArgsForPostJdk11()
    {
        JvmVersion jvmVersion = JvmVersion.create( 11, "" );
        JfrProfiler profiler = new JfrProfiler();
        JvmArgs jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark, Parameters.NONE, null );
        assertThat( jvmArgs.toArgs(), not( hasItem( equalTo( "-XX:+UnlockCommercialFeatures" ) ) ) );
    }
}
