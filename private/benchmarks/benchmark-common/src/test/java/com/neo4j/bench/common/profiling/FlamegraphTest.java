/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public class FlamegraphTest
{
    @Inject
    private TestDirectory tempFolder;

    @Test
    public void removeHeightAndWidthFromSvg() throws Exception
    {
        // given
        Path svgPath = tempFolder.file( "flamegraph.svg" );
        Path flamegraphDir = Paths.get( System.getenv( "FLAMEGRAPH_DIR" ) );
        Path collapsedStack = flamegraphDir.resolve( "test/results/perf-java-stacks-01-collapsed-pid.txt" );
        BenchmarkGroupDirectory benchmarkGroupDirectory =
                BenchmarkGroupDirectory.findOrCreateAt( tempFolder.directory( "benchmark" ), new BenchmarkGroup( "group" ) );
        BenchmarkDirectory benchmarkDirectory =
                benchmarkGroupDirectory.findOrCreate( Benchmark.benchmarkFor( "description", "simpleName", Benchmark.Mode.LATENCY, Collections.emptyMap() ) );
        ForkDirectory forkDirectory = benchmarkDirectory.create( "1" );
        // when
        Flamegraph.createFlamegraphs( forkDirectory, flamegraphDir, collapsedStack, svgPath );
        // then
        assertTrue( Files.isRegularFile( svgPath ) );
//        assertThat( DocumentBuilderFactory.newInstance().newDocumentBuilder().parse( svgPath.toFile() ),
//                    hasXPath( "/svg[not(@width) and not(@height)]" ) );
    }
}
