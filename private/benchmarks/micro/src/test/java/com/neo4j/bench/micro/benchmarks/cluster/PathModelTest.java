/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.data.Stores;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.ByteUnit;

import static org.junit.Assert.assertTrue;

import static java.lang.StrictMath.abs;

public class PathModelTest
{
    @Test
    public void ensureDiffIsWithinExpectedBounds() throws Throwable
    {
        EditionModuleBackedAbstractBenchmark editionModuleBackedAbstractBenchmark = new EditionModuleBackedAbstractBenchmark()
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

        Stores stores = new Stores( Paths.get( "." ) );
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "group1" );
        Benchmark benchmark = Benchmark.benchmarkFor( "test benchmark", "benchmark1", Benchmark.Mode.LATENCY, Collections.emptyMap() );

        editionModuleBackedAbstractBenchmark.benchmarkSetup( benchmarkGroup, benchmark, stores, Neo4jConfig.empty() );

        GraphDatabaseService db = editionModuleBackedAbstractBenchmark.db();

        // model could be improved for small tx size currently they can't keep up with th within 5% diff
        List<Long> larger = Arrays.asList( ByteUnit.kibiBytes( 100 ), ByteUnit.mebiBytes( 1 ), ByteUnit.mebiBytes( 10 ), ByteUnit.mebiBytes( 100 ),
                                           ByteUnit.gibiBytes( 1 ) );
        // on the other and a diff of 50% is not really a problem in this size interval.
        List<Long> smaller = Arrays.asList( ByteUnit.kibiBytes( 1 ), ByteUnit.kibiBytes( 10 ) );

        for ( Long size : larger )
        {
            verifyDiff( editionModuleBackedAbstractBenchmark, db, size, 0.03f );
        }
        for ( Long size : smaller )
        {
            verifyDiff( editionModuleBackedAbstractBenchmark, db, size, 0.6f );
        }
    }

    private void verifyDiff( EditionModuleBackedAbstractBenchmark editionModuleBackedAbstractBenchmark, GraphDatabaseService db, Long size, float maxDiff )
    {
        TxFactory.commitTx( Math.toIntExact( size ), db );

        ClusterTx clusterTx = editionModuleBackedAbstractBenchmark.popLatest();

        double diff = abs( 1.0 - clusterTx.size() / (double) size );

        String message =
                "Expected a diff of less than " + maxDiff * 100 + "%. Got actual diff: " + diff * 100 + "%. Got size: " + clusterTx.size() + " actual wanted " +
                size;

        assertTrue( message, diff < maxDiff );
    }
}
