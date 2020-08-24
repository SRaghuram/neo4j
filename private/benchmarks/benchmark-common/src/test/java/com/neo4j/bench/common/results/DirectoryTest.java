/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.results;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.common.results.RunPhase.MEASUREMENT;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertException;
import static com.neo4j.bench.common.util.BenchmarkUtil.sanitize;
import static com.neo4j.bench.model.model.Benchmark.Mode;
import static com.neo4j.bench.model.model.Parameters.NONE;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public class DirectoryTest
{
    private static final BenchmarkGroup GROUP_1 = new BenchmarkGroup( "group1" );
    private static final Benchmark BENCH_1 = Benchmark.benchmarkFor( "test bench 1", "bench 1", Mode.LATENCY, new HashMap<>() );
    private static final Benchmark BENCH_2 = Benchmark.benchmarkFor( "test bench 2", "bench2", Mode.THROUGHPUT, new HashMap<>() );
    private static final String FORK1 = "fork 1";
    private static final String FORK2 = "fork2";

    @Inject
    public TestDirectory temporaryFolder;

    @Test
    void shouldBeAbleToCreateGroupDirs()
    {
        Path parentDir = temporaryFolder.absolutePath();
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP_1 );

        Path groupDirPath = Paths.get( groupDir.toAbsolutePath() );
        Path expectedGroupDirPath = parentDir.resolve( GROUP_1.name() );
        assertThat( "Group dir had unexpected location", expectedGroupDirPath, equalTo( groupDirPath ) );
        assertTrue( Files.exists( expectedGroupDirPath ), "Group dir was not created" );
        assertThat( "Group dir did not know its group", groupDir.benchmarkGroup(), equalTo( GROUP_1 ) );
    }

    @Test
    void groupDirShouldCreateBenchDirs()
    {
        Path parentDir = temporaryFolder.absolutePath();
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP_1 );

        BenchmarkDirectory benchDir1 = groupDir.findOrCreate( BENCH_1 );
        Path groupDirPath = Paths.get( groupDir.toAbsolutePath() );
        Path benchDirPath1 = Paths.get( benchDir1.toAbsolutePath() );
        Path expectedBenchDirPath1 = groupDirPath.resolve( sanitize( BENCH_1.name() ) );
        assertThat( "Bench dir had unexpected location", expectedBenchDirPath1, equalTo( benchDirPath1 ) );
        assertTrue( Files.exists( expectedBenchDirPath1 ), "Bench dir was not created" );
        assertThat( "Group dir did not contain expected benchmark", groupDir.benchmarks(), containsInAnyOrder( BENCH_1 ) );
        assertThat( "Bench dir did not know its benchmark", benchDir1.benchmark(), equalTo( BENCH_1 ) );

        BenchmarkDirectory benchDir1Copy = groupDir.findOrCreate( BENCH_1 );
        assertThat( "Bench dir with same name did not point to same underlying folder",
                    benchDir1.toAbsolutePath(),
                    equalTo( benchDir1Copy.toAbsolutePath() ) );
        assertThat( "Group dir no longer contains expected benchmark", groupDir.benchmarks(), containsInAnyOrder( BENCH_1 ) );

        groupDir.findOrCreate( BENCH_2 );

        assertThat( "Group dir has different number of benchmark directories than benchmarks",
                    groupDir.benchmarks().size(),
                    equalTo( groupDir.benchmarkDirectories().size() ) );
        assertThat( "Group dir did not contain expected benchmarks", groupDir.benchmarks(), containsInAnyOrder( BENCH_1, BENCH_2 ) );

        groupDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP_1 );
        assertThat( "Group dir did not overwrite directory", groupDir.benchmarks(), equalTo( Collections.emptyList() ) );
    }

    @Test
    void groupDirShouldBeAbleToOverwriteItself()
    {
        Path parentDir = temporaryFolder.absolutePath();
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP_1 );
        assertThat( "Should contain no benchmarks", groupDir.benchmarks(), equalTo( Collections.emptyList() ) );

        groupDir.findOrCreate( BENCH_1 );
        assertThat( "Should contain one, expected benchmark", groupDir.benchmarks(), containsInAnyOrder( BENCH_1 ) );

        groupDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP_1 );
        assertThat( "Should contain no benchmarks", groupDir.benchmarks(), equalTo( Collections.emptyList() ) );
    }

    @Test
    void benchDirShouldCreateForkDirs()
    {
        Path parentDir = temporaryFolder.absolutePath();
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP_1 );

        List<ProfilerType> expectedProfilers1 = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        List<ProfilerType> expectedProfilers2 = Lists.newArrayList( ProfilerType.ASYNC );

        BenchmarkDirectory benchDir = groupDir.findOrCreate( BENCH_1 );

        assertThat( "New bench dir was not empty", benchDir.forks(), equalTo( Collections.emptyList() ) );

        ForkDirectory forkDir1 = benchDir.create( FORK1 );
        Map<RecordingDescriptor,Path> fork1ExpectedRecordings = registerProfilers( GROUP_1,
                                                                                   BENCH_1,
                                                                                   ParameterizedProfiler.defaultProfilers( expectedProfilers1 ),
                                                                                   forkDir1 );
        Path benchDirPath = Paths.get( benchDir.toAbsolutePath() );
        Path forkDirPath1 = Paths.get( forkDir1.toAbsolutePath() );
        Path expectedForkDirPath1 = benchDirPath.resolve( sanitize( FORK1 ) );
        assertThat( "Fork dir had unexpected location", expectedForkDirPath1, equalTo( forkDirPath1 ) );
        assertTrue( Files.exists( expectedForkDirPath1 ), "Fork dir was not created" );
        assertThat( "Fork dir should know its profilers", forkDir1.recordings(), equalTo( fork1ExpectedRecordings ) );
        assertThat( "Fork dir should know its name", forkDir1.name(), equalTo( FORK1 ) );

        ForkDirectory forkDir2 = benchDir.create( FORK2 );
        Map<RecordingDescriptor,Path> fork2ExpectedRecordings = registerProfilers( GROUP_1,
                                                                                   BENCH_1,
                                                                                   ParameterizedProfiler.defaultProfilers( expectedProfilers2 ),
                                                                                   forkDir2 );
        assertThat( "Fork dir should know its profilers", forkDir2.recordings(), equalTo( fork2ExpectedRecordings ) );
        assertThat( "Fork dir should know its name", forkDir2.name(), equalTo( FORK2 ) );

        assertThat( "Bench dir contained unexpected forks",
                    benchDir.forks().stream().map( ForkDirectory::name ).collect( toList() ),
                    containsInAnyOrder( FORK1, FORK2 ) );

        // should not be able to create a fork directory where one already exists
        assertException( RuntimeException.class,
                                       () -> benchDir.create( FORK1 ) );
    }

    @Test
    void benchDirShouldOpenExistingForkDirs()
    {
        Path parentDir = temporaryFolder.absolutePath();
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP_1 );
        BenchmarkDirectory benchDir = groupDir.findOrCreate( BENCH_1 );
        List<ProfilerType> expectedProfilers = Lists.newArrayList( ProfilerType.JFR, ProfilerType.GC );
        ForkDirectory forkDirBefore = benchDir.create( FORK1 );
        assertThat( forkDirBefore.recordings(), equalTo( Collections.emptyMap() ) );
        Map<RecordingDescriptor,Path> expectedRecordings = registerProfilers( GROUP_1,
                                                                              BENCH_1,
                                                                              ParameterizedProfiler.defaultProfilers( expectedProfilers ),
                                                                              forkDirBefore );
        assertThat( forkDirBefore.recordings(), equalTo( expectedRecordings ) );
        ForkDirectory forkDirAfter = ForkDirectory.openAt( Paths.get( forkDirBefore.toAbsolutePath() ) );
        assertThat( forkDirBefore.toAbsolutePath(), equalTo( forkDirAfter.toAbsolutePath() ) );
        assertThat( forkDirBefore.name(), equalTo( forkDirAfter.name() ) );
        assertThat( forkDirBefore.recordings(), equalTo( forkDirAfter.recordings() ) );
    }

    @Test
    void benchDirShouldCreateFilesInForkDir() throws Exception
    {
        Path parentDir = temporaryFolder.absolutePath();

        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( parentDir, GROUP_1 );
        BenchmarkDirectory benchDir = groupDir.findOrCreate( BENCH_1 );
        ForkDirectory forkDir = benchDir.create( FORK1 );
        Path forkDirPath = Paths.get( forkDir.toAbsolutePath() );

        // Should contain 1 fork description file: fork.json
        assertThat( "Fork directory should be empty immediately after creation", Files.list( forkDirPath ).count(), equalTo( 1L ) );

        Path file1 = forkDir.pathFor( "file1" );
        assertFalse( Files.exists( file1 ), "File should not yet be created" );
        assertThat( "File path should be within fork directory", file1.getParent(), equalTo( forkDirPath ) );

        assertException( RuntimeException.class,
                                       () -> forkDir.findOrFail( "file1" ) );

        assertThat( "Same filenames mapped to different files", forkDir.create( "file1" ), equalTo( file1 ) );
        assertTrue( Files.exists( file1 ), "File should be created now" );

        assertException( RuntimeException.class,
                                       () -> forkDir.create( "file1" ) );

        assertThat( "Could not find previously created file", forkDir.findOrFail( "file1" ), equalTo( file1 ) );
        assertThat( "Same filenames mapped to different files", forkDir.findOrCreate( "file1" ), equalTo( file1 ) );

        Path file2 = forkDir.pathFor( "file2" );
        assertFalse( Files.exists( file2 ), "File should not yet be created" );
        assertThat( "Same filenames mapped to different files", forkDir.findOrCreate( "file2" ), equalTo( file2 ) );
        assertTrue( Files.exists( file2 ), "File should be created now" );

        assertThat( "Files created for same fork should have same parent folder", file1.getParent(), equalTo( file2.getParent() ) );

        Path planFile = forkDir.pathForPlan();
        assertThat( "Path to plan file should have correct name", planFile.getFileName().toString(), equalTo( ForkDirectory.PLAN_JSON ) );
        assertFalse( Files.exists( planFile ), "Plan file should not yet be created" );
    }

    private Map<RecordingDescriptor,Path> registerProfilers( BenchmarkGroup benchmarkGroup,
                                                             Benchmark benchmark,
                                                             List<ParameterizedProfiler> profilers,
                                                             ForkDirectory forkDir )
    {
        Map<RecordingDescriptor,Path> recordings = new HashMap<>();
        for ( ParameterizedProfiler profiler : profilers )
        {
            ProfilerRecordingDescriptor profilerDescriptor = ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                                                                 benchmark,
                                                                                                 MEASUREMENT,
                                                                                                 profiler,
                                                                                                 NONE );
            // this call registers the created path with the fork
            RecordingDescriptor recordingDescriptor = profilerDescriptor.recordingDescriptorFor( profiler.profilerType().recordingType() );
            Path recording = forkDir.registerPathFor( recordingDescriptor );
            recordings.put( recordingDescriptor, recording );
        }
        return recordings;
    }
}
