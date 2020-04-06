/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JmhLifecycleTrackerTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final BenchmarkGroup GROUP = new BenchmarkGroup( "group" );
    private static final Benchmark BENCHMARK = Benchmark.benchmarkFor( "desc", "bench", Benchmark.Mode.LATENCY, new HashMap<>() );

    @Test
    public void shouldInitAndReload() throws IOException
    {
        // given
        Path workDir = temporaryFolder.newFolder().toPath();

        // when
        JmhLifecycleTracker before = JmhLifecycleTracker.init( workDir );

        // then
        JmhLifecycleTracker after = JmhLifecycleTracker.load( workDir );

        assertThat( before.jsonFile(), equalTo( after.jsonFile() ) );
        assertThat( before.lifeCycleEvents(), equalTo( after.lifeCycleEvents() ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAllowMultipleRunIds() throws IOException
    {
        // given
        Path workDir = temporaryFolder.newFolder().toPath();
        RunnerParams runnerParams = RunnerParams.create( workDir );

        // when
        JmhLifecycleTracker tracker = JmhLifecycleTracker.init( workDir );
        tracker.addTrial( runnerParams, GROUP, BENCHMARK );

        // then
        tracker.addTrial( runnerParams.copyWithNewRunId(), GROUP, BENCHMARK );
    }

    @Test
    public void shouldUpdateSingleEventLogEntryAndSerialize() throws IOException
    {
        // given
        Path workDir = temporaryFolder.newFolder().toPath();
        RunnerParams runnerParams = RunnerParams.create( workDir )
                                                .copyWithProfilers( ParameterizedProfiler.defaultProfilers( ProfilerType.GC ) );
        JmhLifecycleTracker before = JmhLifecycleTracker.init( workDir );
        List<BenchmarkGroupDirectory> groupsBefore = BenchmarkGroupDirectory.searchAllIn( workDir );
        assertTrue( groupsBefore.isEmpty() );

        // when

        // this call should also persist the event log
        ForkDirectory forkDirectory = before.addTrial( runnerParams, GROUP, BENCHMARK );

        // then
        List<BenchmarkGroupDirectory> groupsAfter = BenchmarkGroupDirectory.searchAllIn( workDir );
        assertThat( groupsAfter.size(), equalTo( 1 ) );
        BenchmarkGroupDirectory groupDir = groupsAfter.get( 0 );
        assertThat( groupDir.benchmarkGroup(), equalTo( GROUP ) );

        List<BenchmarkDirectory> benchDirs = groupDir.benchmarksDirectories();
        assertThat( benchDirs.size(), equalTo( 1 ) );
        BenchmarkDirectory benchDir = benchDirs.get( 0 );
        assertThat( benchDir.benchmark(), equalTo( BENCHMARK ) );

        List<ForkDirectory> forkDirs = benchDir.forks();
        assertThat( forkDirs.size(), equalTo( 1 ) );
        assertThat( forkDirs.get( 0 ).toAbsolutePath(), equalTo( forkDirectory.toAbsolutePath() ) );

        JmhLifecycleTracker after = JmhLifecycleTracker.load( workDir );
        assertThat( before.jsonFile(), equalTo( after.jsonFile() ) );
        assertThat( before.lifeCycleEvents(), equalTo( after.lifeCycleEvents() ) );
    }

    @Test
    public void shouldUpdateMultipleEventLogEntriesWithOneProfiler() throws IOException
    {
        // given
        boolean isForking = true;
        Path workDir = temporaryFolder.newFolder().toPath();
        RunnerParams runnerParams = RunnerParams.create( workDir )
                                                .copyWithProfilers( ParameterizedProfiler.defaultProfilers( ProfilerType.NO_OP ) );
        JmhLifecycleTracker before = JmhLifecycleTracker.init( workDir );

        /*
        Expected Fork Directory creation behavior, given 1 profiler:

        1   create  fork0  profiler
        2   use     fork0  onSetup
        3   create  fork1  profiler
        4   use     fork1  onSetup
        5   create  fork2  profiler
        6   use     fork2  onSetup
        ...
         */

        // when, then

        // (1) new fork directory is created by profiler: fork0
        ForkDirectory forkDir0 = before.addTrial( runnerParams, GROUP, BENCHMARK );

        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.searchAllIn( workDir ).get( 0 );
        BenchmarkDirectory benchDir = groupDir.benchmarksDirectories().get( 0 );

        List<ForkDirectory> forkDirs = benchDir.forks();
        assertThat( forkDirs.size(), equalTo( 1 ) );
        assertThat( forkDirs.get( 0 ).toAbsolutePath(), equalTo( forkDir0.toAbsolutePath() ) );

        // (2) onSetup use fork0
        assertThat( before.getForkDirectory( runnerParams, isForking, GROUP, BENCHMARK ).toAbsolutePath(), equalTo( forkDir0.toAbsolutePath() ) );

        // (3) new fork directory is created by profiler: fork1
        ForkDirectory forkDir1 = before.addTrial( runnerParams, GROUP, BENCHMARK );
        assertThat( forkDir1.toAbsolutePath(), not( equalTo( forkDir0.toAbsolutePath() ) ) );

        // (4) onSetup use fork1
        assertThat( before.getForkDirectory( runnerParams, isForking, GROUP, BENCHMARK ).toAbsolutePath(), equalTo( forkDir1.toAbsolutePath() ) );

        // (5) new fork directory is created by profiler: fork2
        ForkDirectory forkDir2 = before.addTrial( runnerParams, GROUP, BENCHMARK );
        assertThat( forkDir2.toAbsolutePath(), not( equalTo( forkDir1.toAbsolutePath() ) ) );

        // (6) onSetup use fork1
        assertThat( before.getForkDirectory( runnerParams, isForking, GROUP, BENCHMARK ).toAbsolutePath(), equalTo( forkDir2.toAbsolutePath() ) );

        // should load multiple events correctly
        JmhLifecycleTracker after = JmhLifecycleTracker.load( workDir );
        assertThat( before.jsonFile(), equalTo( after.jsonFile() ) );
        assertThat( before.lifeCycleEvents(), equalTo( after.lifeCycleEvents() ) );
    }

    @Test
    public void shouldUpdateMultipleEventLogEntriesWithTwoProfilers() throws IOException
    {
        // given
        Path workDir = temporaryFolder.newFolder().toPath();
        RunnerParams runnerParams = RunnerParams.create( workDir )
                                                .copyWithProfilers( ParameterizedProfiler.defaultProfilers( ProfilerType.GC, ProfilerType.JFR ) );
        JmhLifecycleTracker before = JmhLifecycleTracker.init( workDir );

        /*
        Expected Fork Directory creation behavior, given 2 profilers:

        1   create fork0 (profiler 1)
        2   use    fork0 (profiler 2)
        3   create fork1 (profiler 1)
        4   use    fork1 (profiler 2)
        ...
         */

        // when, then

        // (1) new fork directory is created
        ForkDirectory forkDir1 = before.addTrial( runnerParams, GROUP, BENCHMARK );

        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.searchAllIn( workDir ).get( 0 );
        BenchmarkDirectory benchDir = groupDir.benchmarksDirectories().get( 0 );

        List<ForkDirectory> forkDirs = benchDir.forks();
        assertThat( forkDirs.size(), equalTo( 1 ) );
        assertThat( forkDirs.get( 0 ).toAbsolutePath(), equalTo( forkDir1.toAbsolutePath() ) );

        // (2) same fork directory is returned
        assertThat( before.addTrial( runnerParams, GROUP, BENCHMARK ).toAbsolutePath(), equalTo( forkDir1.toAbsolutePath() ) );

        // (3) new fork directory is created
        ForkDirectory forkDir2 = before.addTrial( runnerParams, GROUP, BENCHMARK );
        assertThat( forkDir2.toAbsolutePath(), not( equalTo( forkDir1.toAbsolutePath() ) ) );

        // (4) same fork directory is returned
        assertThat( before.addTrial( runnerParams, GROUP, BENCHMARK ).toAbsolutePath(), equalTo( forkDir2.toAbsolutePath() ) );

        // should load multiple events correctly
        JmhLifecycleTracker after = JmhLifecycleTracker.load( workDir );
        assertThat( before.jsonFile(), equalTo( after.jsonFile() ) );
        assertThat( before.lifeCycleEvents(), equalTo( after.lifeCycleEvents() ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAllowTrackingWithoutProfilerConfigured() throws IOException
    {
        // given
        Path workDir = temporaryFolder.newFolder().toPath();
        RunnerParams runnerParams = RunnerParams.create( workDir );
        JmhLifecycleTracker before = JmhLifecycleTracker.init( workDir );

        // when, then
        before.addTrial( runnerParams, GROUP, BENCHMARK );
    }

    @Test
    public void shouldResetTracker() throws IOException
    {
        // given
        Path workDir = temporaryFolder.newFolder().toPath();
        RunnerParams runnerParams = RunnerParams.create( workDir )
                                                .copyWithProfilers( ParameterizedProfiler.defaultProfilers( ProfilerType.GC ) );
        JmhLifecycleTracker before = JmhLifecycleTracker.init( workDir );

        before.addTrial( runnerParams, GROUP, BENCHMARK );
        before.addTrial( runnerParams, GROUP, BENCHMARK );
        before.addTrial( runnerParams, GROUP, BENCHMARK );
        before.addTrial( runnerParams, GROUP, BENCHMARK );
        assertFalse( before.lifeCycleEvents().isEmpty() );

        // when
        before.reset();

        // then

        // should clear the event log
        assertTrue( before.lifeCycleEvents().isEmpty() );

        // should also clear the persisted event log
        JmhLifecycleTracker after = JmhLifecycleTracker.load( workDir );
        assertTrue( after.lifeCycleEvents().isEmpty() );
    }
}
