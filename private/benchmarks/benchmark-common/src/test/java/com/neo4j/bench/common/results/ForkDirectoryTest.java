/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.results;

import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
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
import java.util.Map;
import java.util.function.Predicate;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.common.results.RunPhase.MEASUREMENT;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertException;
import static com.neo4j.bench.common.util.BenchmarkUtil.sanitize;
import static com.neo4j.bench.model.model.Parameters.NONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public class ForkDirectoryTest
{
    private static final BenchmarkGroup GROUP = new BenchmarkGroup( "group1" );
    private static final String SIMPLE_NAME_EXCEEDING_FILESYSTEM_LIMIT = "bench 1 " + Strings.repeat( "1234567890", 30 );
    private static final Benchmark BENCH =
            Benchmark.benchmarkFor( "test bench 1", SIMPLE_NAME_EXCEEDING_FILESYSTEM_LIMIT, Benchmark.Mode.LATENCY, Collections.emptyMap() );
    private static final String FORK_NAME = "fork 1";

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void shouldKnowItsLocationAndName()
    {
        BenchmarkDirectory benchDir = BenchmarkGroupDirectory.createAt( temporaryFolder.absolutePath(), GROUP ).findOrCreate( BENCH );
        ForkDirectory forkDir = benchDir.create( FORK_NAME );

        RecordingDescriptor recordingDescriptor = registerProfiler( forkDir, ParameterizedProfiler.defaultProfiler( ProfilerType.JFR ) );

        Path forkDirPath = Paths.get( forkDir.toAbsolutePath() );
        Map<RecordingDescriptor,Path> forkExpectedRecordings =
                ImmutableMap.of( recordingDescriptor, forkDirPath.resolve( recordingDescriptor.sanitizedFilename() ) );

        Path benchDirPath = Paths.get( benchDir.toAbsolutePath() );
        Path expectedForkDirPath = benchDirPath.resolve( sanitize( FORK_NAME ) );
        assertThat( "Fork dir had unexpected location", expectedForkDirPath, equalTo( forkDirPath ) );
        assertTrue( Files.exists( expectedForkDirPath ), "Fork dir was not created" );
        assertThat( "Fork dir should know its profilers", forkDir.recordings(), equalTo( forkExpectedRecordings ) );
        assertThat( "Fork dir should know its name", forkDir.name(), equalTo( FORK_NAME ) );
    }

    @Test
    void shouldNotAllowToCreateForkDirWhenOneAlreadyExists()
    {
        BenchmarkDirectory benchDir = BenchmarkGroupDirectory.createAt( temporaryFolder.absolutePath(), GROUP ).findOrCreate( BENCH );

        benchDir.create( FORK_NAME );

        assertException( RuntimeException.class, () -> benchDir.create( FORK_NAME ) );
    }

    @Test
    void shouldCopyFileAndAddParametersDescriptionFile() throws Exception
    {
        Path targetDir = temporaryFolder.absolutePath();
        ForkDirectory forkDir = BenchmarkGroupDirectory.createAt( targetDir, GROUP ).findOrCreate( BENCH ).create( FORK_NAME );

        RecordingDescriptor recordingDescriptor = registerProfiler( forkDir, ParameterizedProfiler.defaultProfiler( ProfilerType.JFR ) );
        Predicate<RecordingDescriptor> predicate = Predicates.alwaysTrue();

        Map<RecordingDescriptor,Path> recordingDescriptors = forkDir.copyProfilerRecordings( targetDir, predicate );

        assertThat( recordingDescriptors.keySet(), contains( recordingDescriptor ) );
        Path actualPath = recordingDescriptors.get( recordingDescriptor );
        assertThat( "File has a wrong path", actualPath, equalTo( Paths.get( recordingDescriptor.filename() ) ) );
        assertTrue( Files.exists( targetDir.resolve( actualPath ) ), "File was not copied" );
        Path expectedParamsFile = targetDir.resolve( recordingDescriptor.filename() + ".params" );
        assertTrue( Files.exists( expectedParamsFile ), "Parameters file was not created" );
        assertThat( Files.readString( expectedParamsFile ), equalTo( recordingDescriptor.name() ) );
    }

    @Test
    void shouldNotCopyIfPredicateIsFalse()
    {
        Path targetDir = temporaryFolder.absolutePath();
        ForkDirectory forkDir = BenchmarkGroupDirectory.createAt( targetDir, GROUP ).findOrCreate( BENCH ).create( FORK_NAME );

        RecordingDescriptor recordingDescriptor = registerProfiler( forkDir, ParameterizedProfiler.defaultProfiler( ProfilerType.JFR ) );
        Predicate<RecordingDescriptor> predicate = Predicates.alwaysFalse();

        Map<RecordingDescriptor,Path> recordingDescriptors = forkDir.copyProfilerRecordings( targetDir, predicate );

        assertThat( recordingDescriptors.keySet(), empty() );
        assertFalse( Files.exists( targetDir.resolve( recordingDescriptor.filename() ) ), "File was copied" );
    }

    private RecordingDescriptor registerProfiler( ForkDirectory forkDir, ParameterizedProfiler profiler )
    {
        ProfilerRecordingDescriptor profilerDescriptor =
                ProfilerRecordingDescriptor.create( ForkDirectoryTest.GROUP, ForkDirectoryTest.BENCH, MEASUREMENT, profiler, NONE );
        RecordingDescriptor recordingDescriptor = profilerDescriptor.recordingDescriptorFor( profiler.profilerType().recordingType() );
        forkDir.findOrCreate( recordingDescriptor.sanitizedFilename() );
        forkDir.registerPathFor( recordingDescriptor );
        return recordingDescriptor;
    }
}
