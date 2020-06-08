/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.common.process.HasPid;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.util.JsonUtil;
import com.neo4j.bench.jmh.api.profile.AbstractMicroProfiler;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.Profiler;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * The purpose of {@link JmhLifecycleTracker} is to augment the information provided by {@link BenchmarkParams} & {@link IterationParams} with enough additional
 * information so {@link Profiler} & {@link BaseBenchmark} implementations can discover which fork number they are currently at, and to enable them to find
 * the {@link ForkDirectory} associated with that fork.
 * <p>
 * Given a work directory, a {@link JmhLifecycleTracker} will always persist itself to the same file, e.g., work/jmh-lifecycle-log.json.
 * <p>
 * {@link JmhLifecycleTracker#load} discovers & deserializes the lifecycle log within provided work directory.
 * <p>
 * If {@link JmhLifecycleTracker#load} is invoked with a directory that does not yet contain a lifecycle log, it will fail.
 */
public class JmhLifecycleTracker
{
    private static final String FILENAME = "jmh-lifecycle-log.json";

    /**
     * Creates a lifecycle log in the provided work directory.
     * <p>
     * If a lifecycle log already exists in that directory, it will be overwritten.
     *
     * @param workDir directory in which lifecycle log will be created.
     * @return {@link JmhLifecycleTracker} instance representing the newly created (and persisted) lifecycle log.
     */
    static JmhLifecycleTracker init( Path workDir )
    {
        Path jsonFile = workDir.resolve( FILENAME );
        JmhLifecycleTracker jmhLifecycleTracker = new JmhLifecycleTracker( BenchmarkUtil.forceRecreateFile( jsonFile ) );
        jmhLifecycleTracker.persist();
        return jmhLifecycleTracker;
    }

    /**
     * Discovers & deserializes the lifecycle log within provided work directory.
     * <p>
     * If method is invoked with a directory that does not yet contain a lifecycle log, it will fail.
     *
     * @param workDir directory from which lifecycle log will be loaded.
     * @return {@link JmhLifecycleTracker} instance representing the deserialized lifecycle log.
     * @throws RuntimeException if invoked with a directory that does not yet contain a lifecycle log.
     */
    public static JmhLifecycleTracker load( Path workDir )
    {
        Path jsonFile = workDir.resolve( FILENAME );
        BenchmarkUtil.assertFileExists( jsonFile );
        return JsonUtil.deserializeJson( jsonFile, JmhLifecycleTracker.class );
    }

    private final Path jsonFile;
    private final List<LifeCycleEvent> lifeCycleEvents;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    private JmhLifecycleTracker()
    {
        this( null );
    }

    private JmhLifecycleTracker( Path jsonFile )
    {
        this.jsonFile = jsonFile;
        this.lifeCycleEvents = new ArrayList<>();
    }

    Path jsonFile()
    {
        return jsonFile;
    }

    List<LifeCycleEvent> lifeCycleEvents()
    {
        return lifeCycleEvents;
    }

    /**
     * To facilitate correct JMH lifecycle tracking, this method must be called multiple times per fork:
     * <p>
     * <ol>
     * <li>Once per fork, from {@link BaseBenchmark#onSetup}</li>
     * <li>Once per fork per profiler, e.g., from {@link AbstractMicroProfiler#addJVMOptions}</li>
     * </ol>
     *
     * @return a {@link ForkDirectory} instance matching this benchmark execution.
     * <p>
     * Either a newly created {@link ForkDirectory} if this is the first call of a new fork, otherwise a previously created {@link ForkDirectory}.
     */
    public ForkDirectory addTrial( RunnerParams runnerParams,
                                   BenchmarkGroup benchmarkGroup,
                                   Benchmark benchmark )
    {
        assertProfilersNotEmpty( runnerParams );

        lifeCycleEvents.add( new LifeCycleEvent( runnerParams ) );
        persist();

        ForkDirectoryStatus forkDirectoryStatus = computeForkStatusFor( runnerParams );

        BenchmarkGroupDirectory benchmarkGroupDir = BenchmarkGroupDirectory.findOrCreateAt( runnerParams.workDir(), benchmarkGroup );
        BenchmarkDirectory benchmarkDir = benchmarkGroupDir.findOrCreate( benchmark );
        if ( forkDirectoryStatus.isNew )
        {
            return benchmarkDir.create( forkDirectoryStatus.forkName, runnerParams.profilers() );
        }
        else
        {
            return benchmarkDir.findOrFail( forkDirectoryStatus.forkName );
        }
    }

    public ForkDirectory getForkDirectory( RunnerParams runnerParams, boolean isForking, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        assertProfilersNotEmpty( runnerParams );
        BenchmarkGroupDirectory benchmarkGroupDir = BenchmarkGroupDirectory.findOrCreateAt( runnerParams.workDir(), benchmarkGroup );
        BenchmarkDirectory benchmarkDir = benchmarkGroupDir.findOrCreate( benchmark );
        ForkDirectoryStatus forkDirectoryStatus = computeForkStatusFor( runnerParams );
        return isForking
               // when fork count > 0, the fork directory should have been created already
               ? benchmarkDir.findOrFail( forkDirectoryStatus.forkName )
               // when fork count = 0 (special/debug case) the first call into this method will need to create the fork directory too
               : benchmarkDir.findOrCreate( forkDirectoryStatus.forkName, runnerParams.profilers() );
    }

    public void reset()
    {
        lifeCycleEvents.clear();
        persist();
    }

    private static void assertProfilersNotEmpty( RunnerParams runnerParams )
    {
        if ( runnerParams.profilerTypes().isEmpty() )
        {
            throw new IllegalStateException( "At least one profiler must be configured, for JMH lifecycle tracking (and directory creation) to work" );
        }
    }

    private void persist()
    {
        assertEventLogSanity();
        JsonUtil.serializeJson( jsonFile, this );
    }

    /**
     * Computes if a new {@link ForkDirectory} needs to be created, or if a specific existing one should be reused.
     * <p>
     * To understand the logic it is helpful to know that {@link JmhLifecycleTracker#addTrial} is invoked from one of two places,
     * exactly once (by whichever method is invoked first):
     * <ol>
     *     <li>{@link AbstractMicroProfiler#addJVMOptions}</li>
     *     <li>{@link AbstractMicroProfiler#addJVMInvokeOptions}</li>
     * </ol>
     * <p>
     * A new {@link ForkDirectory} will be created the first time {@link JmhLifecycleTracker#addTrial} is called by a fork.
     * By tracking/counting {@link JmhLifecycleTracker#addTrial} invocations, {@link JmhLifecycleTracker} knows if it is in a new fork -- within every fork
     * {@link JmhLifecycleTracker#addTrial} will be called exactly 'profiler count' times.
     */
    private ForkDirectoryStatus computeForkStatusFor( RunnerParams runnerParams )
    {
        int maxEventsPerFork = runnerParams.profilerTypes().size();
        // '-1' is required to make the maths zero-based
        int lifeCycleEventCount = lifeCycleEvents.size() - 1;
        int forkCount = lifeCycleEventCount / maxEventsPerFork;
        String forkName = runnerParams.runId() + "_" + forkCount;
        if ( lifeCycleEventCount % maxEventsPerFork == 0 )
        {
            return new ForkDirectoryStatus( forkName, true );
        }
        else
        {
            return new ForkDirectoryStatus( forkName, false );
        }
    }

    private void assertEventLogSanity()
    {
        long distinctRunIdCount = lifeCycleEvents.stream().map( LifeCycleEvent::runId ).distinct().count();
        if ( distinctRunIdCount > 1 )
        {
            throw new IllegalStateException( format( "Expected to find 0-1 distinct run IDs in JMH lifecycle event log, but found %s\n%s",
                                                     distinctRunIdCount, prettyString() ) );
        }
    }

    private String prettyString()
    {
        return lifeCycleEvents.stream()
                              .map( event -> format( "%s -- %s -- %s", event.runId(), event.pid(), event.profilerTypes() ) )
                              .collect( joining( "\n" ) );
    }

    private static class ForkDirectoryStatus
    {
        private final String forkName;
        private final boolean isNew;

        private ForkDirectoryStatus( String forkName, boolean isNew )
        {
            this.forkName = forkName;
            this.isNew = isNew;
        }
    }

    private static class LifeCycleEvent
    {
        private final String runId;
        private final long pid;
        private final List<ParameterizedProfiler> profilers;

        /**
         * WARNING: Never call this explicitly.
         * No-params constructor is only used for JSON (de)serialization.
         */
        private LifeCycleEvent()
        {
            this.runId = null;
            this.pid = 42L;
            this.profilers = null;
        }

        private LifeCycleEvent( RunnerParams runnerParams )
        {
            this.runId = runnerParams.runId();
            this.pid = HasPid.getPid().get();
            this.profilers = runnerParams.profilers();
        }

        private String runId()
        {
            return runId;
        }

        private long pid()
        {
            return pid;
        }

        private List<ProfilerType> profilerTypes()
        {
            return ParameterizedProfiler.profilerTypes( profilers );
        }

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }
}
