/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster;

import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.StrictMath.abs;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public class PathModelTest
{

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService db;
    private EditionModuleBackedAbstractBenchmark editionModuleBackedAbstractBenchmark;

    @AfterEach
    void cleanupDB() throws Throwable
    {
        editionModuleBackedAbstractBenchmark.benchmarkTearDown();
    }

    @BeforeEach
    void setupDB() throws Throwable
    {
        editionModuleBackedAbstractBenchmark = new EditionModuleBackedAbstractBenchmark()
        {

            @Override
            public void setUp() throws Throwable
            {

            }

            @Override
            public void shutdown()
            {

            }

            @Override
            public String description()
            {
                return "TEST";
            }

            @Override
            public String benchmarkGroup()
            {
                return "TEST";
            }

            @Override
            public boolean isThreadSafe()
            {
                return false;
            }
        };

        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "group1" );
        Benchmark benchmark = Benchmark.benchmarkFor( "test benchmark", "benchmark1", Benchmark.Mode.LATENCY, Collections.emptyMap() );

        Path workDir = testDirectory.absolutePath();
        BenchmarkGroupDirectory benchmarkGroupDirectory = BenchmarkGroupDirectory.createAt( workDir, benchmarkGroup );
        BenchmarkDirectory benchmarkDirectory = benchmarkGroupDirectory.findOrCreate( benchmark );
        ForkDirectory forkDirectory = benchmarkDirectory.create( "fork", Collections.emptyList() );

        editionModuleBackedAbstractBenchmark.benchmarkSetup( benchmarkGroup, benchmark, Neo4jConfig.empty(), forkDirectory );

        db = editionModuleBackedAbstractBenchmark.db();
    }

    @Test
    void ensureDiffIsWithinExpectedBounds()
    {
        // model could be improved for small tx size currently they can't keep up with th within 5% diff
        List<Long> larger = Arrays.asList( ByteUnit.kibiBytes( 100 ), ByteUnit.mebiBytes( 1 ), ByteUnit.mebiBytes( 10 ), ByteUnit.mebiBytes( 100 ) );
        // on the other and a diff of 50% is not really a problem in this size interval.
        List<Long> smaller = Arrays.asList( ByteUnit.kibiBytes( 1 ), ByteUnit.kibiBytes( 10 ) );

        for ( Long size : larger )
        {
            verifyDiff( editionModuleBackedAbstractBenchmark, db, size, 0.03f );
        }
        for ( Long size : smaller )
        {
            verifyDiff( editionModuleBackedAbstractBenchmark, db, size, 0.61f );
        }
    }

    private void verifyDiff( EditionModuleBackedAbstractBenchmark editionModuleBackedAbstractBenchmark, GraphDatabaseService db, Long size, float maxDiff )
    {
        TxFactory.commitTx( Math.toIntExact( size ), db );

        ClusterTx clusterTx = editionModuleBackedAbstractBenchmark.popLatest();

        double diff = abs( 1.0 - clusterTx.size() / (double) size );

        assertTrue( diff < maxDiff,
                    format( "Expected a diff of less than %f%%. Got actual diff: %f%%. Got size: %d actual wanted %d",
                            maxDiff * 100,
                            diff * 100,
                            clusterTx.size(),
                            size ) );
    }
}
