/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.Lists;
import com.neo4j.bench.micro.benchmarks.core.ConcurrentReadWriteLabelsV2;
import com.neo4j.bench.micro.benchmarks.core.ReadById;
import com.neo4j.bench.micro.benchmarks.test.AlwaysCrashes;
import com.neo4j.bench.micro.benchmarks.test.ConstantDataConstantAugment;
import com.neo4j.bench.micro.benchmarks.test.ConstantDataVariableAugment;
import com.neo4j.bench.micro.benchmarks.test.DefaultDisabled;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.micro.config.BenchmarkDescription;
import com.neo4j.bench.micro.config.Validation;
import com.neo4j.bench.micro.data.Stores;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static com.neo4j.bench.micro.config.BenchmarkDescription.of;
import static com.neo4j.bench.micro.profile.ProfileDescriptor.profileTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static java.util.stream.Collectors.toList;

public class InteractiveRunIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldRunExactlyOneMethodOfBenchmarkClass() throws Exception
    {
        Class benchmark = ReadById.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation() );
        int benchmarkMethodCount = benchmarkDescription.methods().size();
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 ) / benchmarkMethodCount;
        // parameters affect store content, in this benchmark
        int expectedStoreCount = benchmarkDescription.storeCount( newArrayList( "format" ) );
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, "randomNodeById" );
    }

    @Test
    public void shouldRunAllMethodsOfBenchmarkClass() throws Exception
    {
        Class benchmark = ReadById.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters DO NOT affect store content, in this benchmark
        int expectedStoreCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL );
    }

    @Test
    public void shouldRunAllMethodsOfGroupBenchmarkClass() throws Exception
    {
        Class benchmark = ConcurrentReadWriteLabelsV2.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters affect store content, in this benchmark
        int expectedStoreCount = benchmarkDescription.storeCount( newArrayList( "format", "count" ) );
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL );
    }

    @Test
    public void shouldRunConstantDataConstantAugment() throws Exception
    {
        Class benchmark = ConstantDataConstantAugment.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters DO NOT affect store content, in this benchmark
        int expectedStoreCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL );
    }

    @Test
    public void shouldRunConstantDataVariableAugment() throws Exception
    {
        Class benchmark = ConstantDataVariableAugment.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters affect store content, in this benchmark
        int expectedStoreCount = expectedBenchmarkCount;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL );
    }

    @Test
    public void shouldRunBenchmarkThatIsDisabledByDefault() throws Exception
    {
        Class benchmark = DefaultDisabled.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters DO NOT affect store content, in this benchmark
        int expectedStoreCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL );
    }

    @Test
    public void shouldNotThrowExceptionWhenErrorPolicyIsSkip() throws Exception
    {
        Class benchmark = AlwaysCrashes.class;
        // no benchmarks will complete, so no profiler recordings will be created
        int expectedBenchmarkCount = 0;
        // parameters DO NOT affect store content, in this benchmark
        // even though benchmark will crash, it will do so after the store was already created
        int expectedStoreCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.SKIP );
    }

    @Test
    public void shouldThrowExceptionWhenErrorPolicyIsFail()
    {
        Class benchmark = AlwaysCrashes.class;
        // no benchmarks will complete, so no profiler recordings will be created
        int expectedBenchmarkCount = 0;
        // parameters DO NOT affect store content, in this benchmark
        // even though benchmark will crash, it will do so after the store was already created
        int expectedStoreCount = 1;
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL ) );
    }

    private void runInteractively(
            Class benchmark,
            int expectedBenchmarkCount,
            int expectedStoreCount,
            ErrorPolicy errorPolicy,
            String... methods ) throws Exception
    {
        File storesDir = temporaryFolder.newFolder();
        Path profilerRecordingDirectory = temporaryFolder.newFolder().toPath();
        boolean generateStoresInFork = true;
        int measurementForks = 1;
        Main.run(
                benchmark,
                generateStoresInFork,
                measurementForks,
                profileTo( profilerRecordingDirectory, Lists.newArrayList( ProfilerType.JFR ) ),
                new Stores( storesDir.toPath() ),
                errorPolicy,
                Paths.get( Jvm.defaultJvmOrFail().launchJava() ),
                methods );

        // for each variant/execution of the enabled benchmark one JFR recording file should be created
        // asserts that:
        // (1) JFR recordings are created
        // (2) only executions for the enabled benchmark is actually run
        // (3) for each JFR recording file a FlameGraph should be created
        int jfrCount = ProfilerTestUtil.recordingCountIn( profilerRecordingDirectory, RecordingType.JFR );
        assertThat( jfrCount, equalTo( expectedBenchmarkCount ) );
        int jfrFlameGraphCount = ProfilerTestUtil.recordingCountIn( profilerRecordingDirectory, RecordingType.JFR_FLAMEGRAPH );
        assertThat( jfrFlameGraphCount, equalTo( expectedBenchmarkCount ) );

        // expected number of stores are present
        try ( Stream<Path> paths = Files.list( storesDir.toPath() ) )
        {
            List<String> pathNames = paths
                    .filter( Files::isDirectory )
                    .filter( Stores::isTopLevelDir )
                    .map( p ->
                          {
                              System.out.println( "DB : " + p );
                              return p;
                          } )
                    .map( Path::toString )
                    .collect( toList() );
            assertThat( "Found: " + pathNames, pathNames.size(), equalTo( expectedStoreCount ) );
        }
    }
}
