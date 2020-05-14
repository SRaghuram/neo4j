/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StoresTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void storeUsageShouldBeEmptyWhenFirstCreated() throws IOException
    {
        Path workDir = temporaryFolder.newFolder().toPath();
        Stores.StoreUsage storeUsage = Stores.StoreUsage.loadOrCreateIfAbsent( workDir );

        assertThat( storeUsage.allStoreBenchmarkInfo().isEmpty(), is( true ) );
    }

    @Test
    public void storeUsageShouldRegister() throws IOException
    {
        Path workDir = temporaryFolder.newFolder().toPath();
        Stores.StoreUsage storeUsage = Stores.StoreUsage.loadOrCreateIfAbsent( workDir );

        String store1 = randomUUID().toString();
        BenchmarkGroup benchmarkGroup1 = randomBenchmarkGroup();
        Benchmark benchmark1a = randomBenchmark();
        storeUsage.register( store1, benchmarkGroup1, benchmark1a );

        assertThat( storeUsage.allStoreBenchmarkInfo().keySet().size(), is( 1 ) );
        assertThat( storeUsage.benchmarksUsingStore( store1 ), containsInAnyOrder( FullBenchmarkName.from( benchmarkGroup1, benchmark1a ).name() ) );

        Benchmark benchmark1b = randomBenchmark();
        storeUsage.register( store1, benchmarkGroup1, benchmark1b );

        assertThat( storeUsage.allStoreBenchmarkInfo().keySet().size(), is( 1 ) );
        assertThat( storeUsage.benchmarksUsingStore( store1 ), containsInAnyOrder( FullBenchmarkName.from( benchmarkGroup1, benchmark1a ).name(),
                                                                                   FullBenchmarkName.from( benchmarkGroup1, benchmark1b ).name() ) );

        String store2 = randomUUID().toString();
        BenchmarkGroup benchmarkGroup2 = randomBenchmarkGroup();
        Benchmark benchmark2a = randomBenchmark();
        storeUsage.register( store2, benchmarkGroup2, benchmark2a );

        assertThat( storeUsage.allStoreBenchmarkInfo().keySet().size(), is( 2 ) );
        assertThat( storeUsage.benchmarksUsingStore( store1 ), containsInAnyOrder( FullBenchmarkName.from( benchmarkGroup1, benchmark1a ).name(),
                                                                                   FullBenchmarkName.from( benchmarkGroup1, benchmark1b ).name() ) );
        assertThat( storeUsage.benchmarksUsingStore( store2 ), containsInAnyOrder( FullBenchmarkName.from( benchmarkGroup2, benchmark2a ).name() ) );
    }

    @Test
    public void storeUsageShouldBeTheSameAfterReload() throws IOException
    {
        Path workDir = temporaryFolder.newFolder().toPath();
        Stores.StoreUsage storeUsageBefore = Stores.StoreUsage.loadOrCreateIfAbsent( workDir );

        String store = randomUUID().toString();
        BenchmarkGroup benchmarkGroup = randomBenchmarkGroup();
        Benchmark benchmark = randomBenchmark();
        storeUsageBefore.register( store, benchmarkGroup, benchmark );

        assertThat( storeUsageBefore.allStoreBenchmarkInfo().keySet().size(), is( 1 ) );
        assertThat( storeUsageBefore.benchmarksUsingStore( store ), containsInAnyOrder( FullBenchmarkName.from( benchmarkGroup, benchmark ).name() ) );

        Stores.StoreUsage storeUsageAfter = Stores.StoreUsage.loadOrCreateIfAbsent( workDir );

        assertThat( storeUsageAfter.allStoreBenchmarkInfo().keySet().size(), is( 1 ) );
        assertThat( storeUsageAfter.benchmarksUsingStore( store ), containsInAnyOrder( FullBenchmarkName.from( benchmarkGroup, benchmark ).name() ) );

        assertThat( storeUsageAfter.allStoreBenchmarkInfo(), equalTo( storeUsageBefore.allStoreBenchmarkInfo() ) );
    }

    @Test( expected = IllegalStateException.class )
    public void storeUsageShouldNotAllowRegisteringSameBenchmarkTwice() throws IOException
    {
        Path workDir = temporaryFolder.newFolder().toPath();
        Stores.StoreUsage storeUsageBefore = Stores.StoreUsage.loadOrCreateIfAbsent( workDir );

        String store = randomUUID().toString();
        BenchmarkGroup benchmarkGroup = randomBenchmarkGroup();
        Benchmark benchmark = randomBenchmark();
        storeUsageBefore.register( store, benchmarkGroup, benchmark );

        storeUsageBefore.register( store, benchmarkGroup, benchmark );
    }

    private static BenchmarkGroup randomBenchmarkGroup()
    {
        return new BenchmarkGroup( randomUUID().toString() );
    }

    private static Benchmark randomBenchmark()
    {
        return Benchmark.benchmarkFor( "description " + randomUUID().toString(),
                                       "name_" + randomUUID().toString(),
                                       Benchmark.Mode.LATENCY,
                                       Collections.singletonMap( "param_" + randomUUID().toString(), "value_" + randomUUID().toString() ) );
    }
}
