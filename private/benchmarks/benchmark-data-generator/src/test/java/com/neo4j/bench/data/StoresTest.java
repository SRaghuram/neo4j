/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;

@TestDirectoryExtension
public class StoresTest
{
    private static final String STORE = randomUUID().toString();
    private static final String COPY = randomUUID().toString();
    private static final BenchmarkGroup BENCHMARK_GROUP = randomBenchmarkGroup();

    @Inject
    public TestDirectory temporaryFolder;

    @Test
    public void storeUsageShouldBeEmptyWhenFirstCreated()
    {
        Path workDir = temporaryFolder.directory( "dir" );
        Stores.StoreUsage storeUsage = new Stores( workDir ).loadStoreUsage();

        assertThat( storeUsage.allStoreBenchmarkInfo( workDir ).entrySet(), empty() );
    }

    @Test
    public void storeUsageShouldRegister()
    {
        Path workDir = temporaryFolder.directory( "dir" );
        Stores stores = new Stores( workDir );

        BenchmarkGroup benchmarkGroup1 = randomBenchmarkGroup();
        Benchmark benchmark1a = randomBenchmark();
        Benchmark benchmark1b = randomBenchmark();
        BenchmarkGroup benchmarkGroup2 = randomBenchmarkGroup();
        Benchmark benchmark2a = randomBenchmark();

        try ( Stores.StoreAndConfig ignored = prepareReusableDb( workDir, benchmarkGroup1, benchmark1a, 10 ) )
        {
        }
        try ( Stores.StoreAndConfig ignored = prepareReusableDb( workDir, benchmarkGroup1, benchmark1b, 10 ) )
        {
        }
        try ( Stores.StoreAndConfig ignored = prepareReusableDb( workDir, benchmarkGroup2, benchmark2a, 20 ) )
        {
        }

        Map<Path,Set<String>> storeUsage = stores.loadStoreUsage().allStoreBenchmarkInfo( workDir );
        assertThat( storeUsage.keySet(), iterableWithSize( 2 ) );
        assertThat( storeUsage, hasValue( Sets.newHashSet(
                FullBenchmarkName.from( benchmarkGroup1, benchmark1a ).name(),
                FullBenchmarkName.from( benchmarkGroup1, benchmark1b ).name()
        ) ) );
        assertThat( storeUsage, hasValue( Sets.newHashSet(
                FullBenchmarkName.from( benchmarkGroup2, benchmark2a ).name()
        ) ) );
    }

    @Test
    public void storeUsageShouldBeTheSameAfterReload()
    {
        Path workDir = temporaryFolder.directory( "dir" );
        Path store = workDir.resolve( STORE );
        Path copy = workDir.resolve( COPY );
        Benchmark benchmark = randomBenchmark();

        Stores.StoreUsage storeUsageBefore = new Stores( workDir ).loadStoreUsage();
        storeUsageBefore.registerPrimary( store );
        storeUsageBefore.registerSecondary( store, copy );
        storeUsageBefore.setInProgress( copy, BENCHMARK_GROUP, benchmark );

        Stores.StoreUsage storeUsageAfter = new Stores( workDir ).loadStoreUsage();

        assertThat( storeUsageAfter.allStoreBenchmarkInfo( workDir ), equalTo( storeUsageBefore.allStoreBenchmarkInfo( workDir ) ) );
        assertThat( storeUsageAfter.allStoreBenchmarkInfo( workDir ), equalTo( ImmutableMap.of(
                store, ImmutableSet.of( FullBenchmarkName.from( BENCHMARK_GROUP, benchmark ).name() )
        ) ) );
    }

    @Test
    public void storeUsageShouldNotAllowRegisteringSameStoreTwice()
    {
        Path workDir = temporaryFolder.directory( "dir" );
        Path store = workDir.resolve( STORE );
        Stores stores = new Stores( workDir );
        Stores.StoreUsage storeUsage = stores.loadStoreUsage();

        storeUsage.registerPrimary( store );

        BenchmarkUtil.assertException( IllegalStateException.class, () ->
                storeUsage.registerPrimary( store )
        );
    }

    @Test
    public void storeShouldNotAllowToSetTwoBenchmarksInProgress()
    {
        Path workDir = temporaryFolder.directory( "dir" );
        Stores.StoreUsage storeUsage = new Stores( workDir ).loadStoreUsage();
        Path store = workDir.resolve( STORE );
        Path copy = workDir.resolve( COPY );

        storeUsage.registerPrimary( store );
        storeUsage.registerSecondary( store, copy );
        storeUsage.setInProgress( copy, BENCHMARK_GROUP, randomBenchmark() );

        BenchmarkUtil.assertException( IllegalStateException.class, () ->
                storeUsage.setInProgress( store, BENCHMARK_GROUP, randomBenchmark() )
        );
    }

    @Test
    public void shouldNotAllowToClearInProgressByOtherStore()
    {
        Path workDir = temporaryFolder.directory( "dir" );
        Stores.StoreUsage storeUsage = new Stores( workDir ).loadStoreUsage();

        BenchmarkUtil.assertException( IllegalStateException.class, () ->
                storeUsage.clearInProgress( workDir.resolve( STORE ) )
        );
    }

    @Test
    public void shouldReuseStore()
    {
        Path workDir = temporaryFolder.directory( "dir" );

        Stores.StoreAndConfig reusableDb1;
        Stores.StoreAndConfig reusableDb2;
        DataGeneratorConfig reusableDb2Config;
        try ( Stores.StoreAndConfig storeAndConfig = prepareReusableDb( workDir, randomBenchmark() ) )
        {
            reusableDb1 = storeAndConfig;
        }
        try ( Stores.StoreAndConfig storeAndConfig = prepareReusableDb( workDir, randomBenchmark() ) )
        {
            reusableDb2 = storeAndConfig;
            reusableDb2Config = readConfig( reusableDb2 );
        }

        assertThat( reusableDb2.store().topLevelDirectory(), equalTo( reusableDb1.store().topLevelDirectory() ) );
        assertThat( readPrimaryStoreConfig( workDir ), equalTo( reusableDb2Config ) );
    }

    @Test
    public void shouldCopyNonReusableStore()
    {
        Path workDir = temporaryFolder.directory( "dir" );

        Stores.StoreAndConfig nonReusableDb1;
        Stores.StoreAndConfig nonReusableDb2;
        DataGeneratorConfig nonReusableDb2Config;
        try ( Stores.StoreAndConfig storeAndConfig = prepareNonReusableDb( workDir, randomBenchmark() ) )
        {
            nonReusableDb1 = storeAndConfig;
        }
        try ( Stores.StoreAndConfig storeAndConfig = prepareNonReusableDb( workDir, randomBenchmark() ) )
        {
            nonReusableDb2 = storeAndConfig;
            nonReusableDb2Config = readConfig( nonReusableDb2 );
        }

        assertThat( nonReusableDb1.store().topLevelDirectory(), not( equalTo( nonReusableDb2.store().topLevelDirectory() ) ) );
        assertThat( readPrimaryStoreConfig( workDir ), equalTo( nonReusableDb2Config ) );
    }

    @Test
    public void shouldRecreateFailedReusableStore()
    {
        Path workDir = temporaryFolder.directory( "dir" );

        Stores.StoreAndConfig failingDb = prepareReusableDb( workDir, randomBenchmark() );
        // not closed e.g. because of an exception in benchmark
        Stores.StoreAndConfig anotherDb;
        DataGeneratorConfig anotherDbConfig;
        try ( Stores.StoreAndConfig storeAndConfig = prepareReusableDb( workDir, randomBenchmark() ) )
        {
            anotherDb = storeAndConfig;
            anotherDbConfig = readConfig( anotherDb );
        }

        assertThat( failingDb.store().topLevelDirectory(), not( anotherDb.store().topLevelDirectory() ) );
        assertThat( readPrimaryStoreConfig( workDir ), equalTo( anotherDbConfig ) );
    }

    @Test
    public void shouldNotChangeReusableDbToNonReusable()
    {
        Path workDir = temporaryFolder.directory( "dir" );

        Stores.StoreAndConfig reusableDb1;
        Stores.StoreAndConfig nonReusableDb2;
        Stores.StoreAndConfig reusableDb3;
        try ( Stores.StoreAndConfig storeAndConfig = prepareReusableDb( workDir, randomBenchmark() ) )
        {
            reusableDb1 = storeAndConfig;
        }
        try ( Stores.StoreAndConfig storeAndConfig = prepareNonReusableDb( workDir, randomBenchmark() ) )
        {
            nonReusableDb2 = storeAndConfig;
        }
        try ( Stores.StoreAndConfig storeAndConfig = prepareReusableDb( workDir, randomBenchmark() ) )
        {
            reusableDb3 = storeAndConfig;
        }

        assertThat( nonReusableDb2.store().topLevelDirectory(), not( equalTo( reusableDb1.store().topLevelDirectory() ) ) );
        assertThat( reusableDb3.store().topLevelDirectory(), equalTo( reusableDb1.store().topLevelDirectory() ) );
    }

    @Test
    public void shouldDeleteFailedStores() throws Exception
    {
        Path workDir = temporaryFolder.directory( "dir" );
        Stores stores = new Stores( workDir );

        prepareReusableDb( workDir, randomBenchmark() );
        // not closed e.g. because of an exception in benchmark
        stores.deleteFailedStores();

        assertThat( findStores( workDir ), iterableWithSize( 1 ) );
    }

    private Stores.StoreAndConfig prepareReusableDb( Path workDir, Benchmark benchmark )
    {
        return prepareReusableDb( workDir, BENCHMARK_GROUP, benchmark, 1 );
    }

    private Stores.StoreAndConfig prepareReusableDb( Path workDir, BenchmarkGroup group, Benchmark benchmark, int nodeCount )
    {
        return prepareDb( workDir, group, benchmark, true, nodeCount );
    }

    private Stores.StoreAndConfig prepareNonReusableDb( Path workDir, Benchmark benchmark )
    {
        return prepareDb( workDir, BENCHMARK_GROUP, benchmark, false, 1 );
    }

    private Stores.StoreAndConfig prepareDb( Path workDir, BenchmarkGroup group, Benchmark benchmark, boolean isReusable, int nodeCount )
    {
        Path neo4jConfig;
        try
        {
            neo4jConfig = File.createTempFile( "neo4j-", ".config", workDir.toFile() ).toPath();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfig );
        DataGeneratorConfig dataGeneratorConfig = new DataGeneratorConfigBuilder()
                .withNodeCount( nodeCount )
                .isReusableStore( isReusable )
                .build();
        return new Stores( workDir ).prepareDb(
                dataGeneratorConfig,
                group,
                benchmark,
                new Augmenterizer.NullAugmenterizer(),
                neo4jConfig,
                1
        );
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

    private DataGeneratorConfig readPrimaryStoreConfig( Path workDir )
    {
        Set<Path> primaryStores = new Stores( workDir ).loadStoreUsage().allStoreBenchmarkInfo( workDir ).keySet();
        assertThat( primaryStores, iterableWithSize( 1 ) );
        Path primaryStore = primaryStores.iterator().next();
        return readConfig( primaryStore );
    }

    private Set<Path> findStores( Path workDir ) throws IOException
    {
        try ( Stream<Path> list = Files.list( workDir ) )
        {
            return list.filter( Stores::isTopLevelDir )
                       .collect( Collectors.toSet() );
        }
    }

    private DataGeneratorConfig readConfig( Stores.StoreAndConfig storeAndConfig )
    {
        return readConfig( storeAndConfig.store().topLevelDirectory() );
    }

    private DataGeneratorConfig readConfig( Path store )
    {
        return DataGeneratorConfig.from( store.resolve( Stores.CONFIG_FILENAME ) );
    }
}
