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
import com.neo4j.bench.client.util.TestDirectorySupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

@ExtendWith( TestDirectoryExtension.class )
public class JfrProfilerTest
{
    @Inject
    public TestDirectory tempFolder;

    private ForkDirectory forkDirectory;
    private BenchmarkGroup benchmarkGroup;
    private Benchmark benchmark;

    @BeforeEach
    public void setUp() throws Exception
    {
        Path parentDir = TestDirectorySupport.createTempDirectoryPath( tempFolder.absolutePath() );

        benchmarkGroup = new BenchmarkGroup( "group" );
        benchmark = Benchmark.benchmarkFor( "description", "simpleName", Mode.LATENCY, Collections.emptyMap() );

        BenchmarkGroupDirectory groupDirectory = BenchmarkGroupDirectory.createAt( parentDir, benchmarkGroup );
        BenchmarkDirectory benchmarkDirectory = groupDirectory.findOrCreate( benchmark );
        forkDirectory = benchmarkDirectory.create( "fork", Collections.emptyList() );
    }

    @Test
    public void jvmArgsForPreJdk11() throws Exception
    {
        JvmVersion jvmVersion = JvmVersion.create( 8, JvmVersion.JAVA_TM_SE_RUNTIME_ENVIRONMENT);
        JfrProfiler profiler = new JfrProfiler();
        List<String> jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark );
        assertThat( jvmArgs, hasItem( equalTo( "-XX:+UnlockCommercialFeatures" ) ) );
    }

    @Test
    public void jvmArgsForPostJdk11() throws Exception
    {
        JvmVersion jvmVersion = JvmVersion.create( 11, "" );
        JfrProfiler profiler = new JfrProfiler();
        List<String> jvmArgs = profiler.jvmArgs( jvmVersion, forkDirectory, benchmarkGroup, benchmark );
        assertThat( jvmArgs, not( hasItem( equalTo( "-XX:+UnlockCommercialFeatures" ) ) ) );
    }
}
