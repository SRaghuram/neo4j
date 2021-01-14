/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.BaseBenchmark;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import com.neo4j.bench.micro.benchmarks.core.ConcurrentReadWriteLabelsV2;
import com.neo4j.bench.micro.benchmarks.core.ReadById;
import com.neo4j.bench.micro.benchmarks.cypher.AllNodesScan;
import com.neo4j.bench.micro.benchmarks.test.AlwaysCrashes;
import com.neo4j.bench.micro.benchmarks.test.ConstantDataConstantAugment;
import com.neo4j.bench.micro.benchmarks.test.ConstantDataVariableAugment;
import com.neo4j.bench.micro.benchmarks.test.DefaultDisabled;
import com.neo4j.bench.data.Stores;
import com.neo4j.bench.model.profiling.RecordingType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.google.common.collect.Lists.newArrayList;
import static com.neo4j.bench.jmh.api.config.BenchmarkDescription.of;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class InteractiveRunIT extends AnnotationsFixture
{
    private static final Logger LOG = LoggerFactory.getLogger( InteractiveRunIT.class );

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void shouldRunExactlyOneMethodOfBenchmarkClassZeroFork() throws Exception
    {
        Class<? extends BaseBenchmark> benchmark = ReadById.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation(), getAnnotations() );
        int benchmarkMethodCount = benchmarkDescription.methods().size();
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 ) / benchmarkMethodCount;
        // parameters affect store content, in this benchmark
        int expectedStoreCount = benchmarkDescription.storeCount( newArrayList( "format" ) ) * 2;
        int forkCount = 0;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, forkCount, "randomNodeById" );
    }

    @Test
    void shouldRunExactlyOneMethodOfBenchmarkClassOneFork() throws Exception
    {
        Class<? extends BaseBenchmark> benchmark = ReadById.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation(), getAnnotations() );
        int benchmarkMethodCount = benchmarkDescription.methods().size();
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 ) / benchmarkMethodCount;
        // parameters affect store content, in this benchmark
        int expectedStoreCount = benchmarkDescription.storeCount( newArrayList( "format" ) ) * 2;
        int forkCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, forkCount, "randomNodeById" );
    }

    @Test
    void shouldRunAllMethodsOfBenchmarkClassZeroFork() throws Exception
    {
        Class<? extends BaseBenchmark> benchmark = AllNodesScan.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation(), getAnnotations() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters affect store content, in this benchmark
        int expectedStoreCount = benchmarkDescription.storeCount( newArrayList( "auth" ) ) * 2;
        int forkCount = 0;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, forkCount );
    }

    @Test
    void shouldRunAllMethodsOfBenchmarkClassOneFork() throws Exception
    {
        Class<? extends BaseBenchmark> benchmark = AllNodesScan.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation(), getAnnotations() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters affect store content, in this benchmark
        int expectedStoreCount = benchmarkDescription.storeCount( newArrayList( "auth" ) ) * 2;
        int forkCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, forkCount );
    }

    @Test
    void shouldRunAllMethodsOfGroupBenchmarkClass() throws Exception
    {
        Class<? extends BaseBenchmark> benchmark = ConcurrentReadWriteLabelsV2.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation(), getAnnotations() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters affect store content, in this benchmark
        int expectedStoreCount = benchmarkDescription.storeCount( newArrayList( "format", "count" ) );
        int forkCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, forkCount );
    }

    @Test
    void shouldRunConstantDataConstantAugment() throws Exception
    {
        Class<? extends BaseBenchmark> benchmark = ConstantDataConstantAugment.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation(), getTestOnlyAnnotations() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters DO NOT affect store content, in this benchmark
        int expectedStoreCount = 2;
        int forkCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, forkCount );
    }

    @Test
    void shouldRunConstantDataVariableAugment() throws Exception
    {
        Class<? extends BaseBenchmark> benchmark = ConstantDataVariableAugment.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation(), getTestOnlyAnnotations() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // parameters affect store content, in this benchmark
        int expectedStoreCount = expectedBenchmarkCount * 2;
        int forkCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, forkCount );
    }

    @Test
    void shouldRunBenchmarkThatIsDisabledByDefault() throws Exception
    {
        Class<? extends BaseBenchmark> benchmark = DefaultDisabled.class;
        BenchmarkDescription benchmarkDescription = of( benchmark, new Validation(), getTestOnlyAnnotations() );
        int expectedBenchmarkCount = benchmarkDescription.executionCount( 1 );
        // extends BaseRegularBenchmark not BaseDatabaseBenchmark, so no store should be created
        int expectedStoreCount = 0;
        int forkCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, forkCount );
    }

    @Test
    void shouldNotThrowExceptionWhenErrorPolicyIsSkip() throws Exception
    {
        Class<? extends BaseBenchmark> benchmark = AlwaysCrashes.class;
        // no benchmarks will complete, so no profiler recordings will be created
        int expectedBenchmarkCount = 0;
        // parameters DO NOT affect store content, in this benchmark
        // even though benchmark will crash, it will do so after the store was already created
        int expectedStoreCount = 1;
        int forkCount = 1;
        runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.SKIP, forkCount );
    }

    @Test
    void shouldThrowExceptionWhenErrorPolicyIsFail()
    {
        Class<? extends BaseBenchmark> benchmark = AlwaysCrashes.class;
        // no benchmarks will complete, so no profiler recordings will be created
        int expectedBenchmarkCount = 0;
        // parameters DO NOT affect store content, in this benchmark
        // even though benchmark will crash, it will do so after the store was already created
        int expectedStoreCount = 1;
        int forkCount = 1;
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> runInteractively( benchmark, expectedBenchmarkCount, expectedStoreCount, ErrorPolicy.FAIL, forkCount ) );
    }

    private void runInteractively(
            Class<? extends BaseBenchmark> benchmark,
            int expectedBenchmarkCount,
            int expectedStoreCount,
            ErrorPolicy errorPolicy,
            int measurementForks,
            String... methods ) throws Exception
    {
        File workDir = temporaryFolder.directory( UUID.randomUUID().toString() ).toFile();
        int iterationCount = 1;
        TimeValue iterationDuration = TimeValue.seconds( 1 );
        Main.run(
                benchmark,
                measurementForks,
                iterationCount,
                iterationDuration,
                ParameterizedProfiler.defaultProfilers( ProfilerType.JFR ),
                workDir.toPath(),
                errorPolicy,
                Paths.get( Jvm.defaultJvmOrFail().launchJava() ),
                methods );

        // for each variant/execution of the enabled benchmark one JFR recording file should be created
        // asserts that:
        // (1) JFR recordings are created
        // (2) only executions for the enabled benchmark is actually run
        // (3) for each JFR recording file a FlameGraph should be created
        int jfrCount = ProfilerTestUtil.recordingCountIn( workDir.toPath(), RecordingType.JFR );
        assertThat( jfrCount, equalTo( expectedBenchmarkCount ) );
        // in 4.0 we do NOT generate Flamegraphs
        int jfrFlameGraphCount = ProfilerTestUtil.recordingCountIn( workDir.toPath(), RecordingType.JFR_FLAMEGRAPH );
        assertThat( jfrFlameGraphCount, equalTo( 0 ) );

        // expected number of stores are present
        try ( Stream<Path> paths = Files.walk( workDir.toPath() ) )
        {
            List<String> pathNames = paths
                    .filter( Files::isDirectory )
                    .filter( Stores::isTopLevelDir )
                    .peek( p -> LOG.debug( "DB : " + p ) )
                    .map( Path::toString )
                    .collect( toList() );
            assertThat( "Found: " + pathNames, pathNames.size(), equalTo( expectedStoreCount ) );
        }
    }
}
