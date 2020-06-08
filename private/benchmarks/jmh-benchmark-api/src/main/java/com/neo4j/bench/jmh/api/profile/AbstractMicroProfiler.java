/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.profile;

import com.neo4j.bench.common.process.HasPid;
import com.neo4j.bench.common.profiling.Profiler;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.JmhLifecycleTracker;
import com.neo4j.bench.jmh.api.RunnerParams;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.ExternalProfiler;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.runner.IterationType;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

import static com.neo4j.bench.jmh.api.BenchmarkDiscoveryUtils.toBenchmarkGroup;
import static com.neo4j.bench.jmh.api.BenchmarkDiscoveryUtils.toBenchmarks;

public abstract class AbstractMicroProfiler implements InternalProfiler, ExternalProfiler
{
    public static Class<? extends AbstractMicroProfiler> toJmhProfiler( ProfilerType profilerType )
    {
        switch ( profilerType )
        {
        case JFR:
            return JfrProfiler.class;
        case ASYNC:
            return AsyncProfiler.class;
        case GC:
            return GcProfiler.class;
        default:
            throw new RuntimeException( "There is no JMH profiler for profiler type: " + profilerType );
        }
    }

    private ProfilerType profilerType;
    private Profiler innerProfiler;
    private ForkDirectory forkDir;

    private int iteration;

    AbstractMicroProfiler()
    {
        super();
        profilerType = profilerType();
        innerProfiler = profilerType.create();
    }

    abstract ProfilerType profilerType();

    private ForkDirectory getOrCreateForkDir( RunnerParams runnerParams,
                                              BenchmarkGroup benchmarkGroup,
                                              Benchmark benchmark )
    {
        if ( null == forkDir )
        {
            JmhLifecycleTracker jmhLifecycleTracker = JmhLifecycleTracker.load( runnerParams.workDir() );
            forkDir = jmhLifecycleTracker.addTrial( runnerParams, benchmarkGroup, benchmark );
        }
        return forkDir;
    }

    private ForkDirectory getOrFailForkDir( RunnerParams runnerParams,
                                            BenchmarkParams benchmarkParams,
                                            BenchmarkGroup benchmarkGroup,
                                            Benchmark benchmark )
    {
        if ( null == forkDir )
        {
            JmhLifecycleTracker jmhLifecycleTracker = JmhLifecycleTracker.load( runnerParams.workDir() );
            boolean isForking = benchmarkParams.getForks() > 0;
            forkDir = jmhLifecycleTracker.getForkDirectory( runnerParams, isForking, benchmarkGroup, benchmark );
        }
        return forkDir;
    }

    /**
     * NOTE: is called from parent process
     */
    @Override
    public Collection<String> addJVMInvokeOptions( BenchmarkParams params )
    {
        RunnerParams runnerParams = RunnerParams.extractFrom( params );
        BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
        Benchmark benchmark = extractBenchmark( params );
        ForkDirectory forkDir = getOrCreateForkDir( runnerParams, benchmarkGroup, benchmark );
        if ( profilerType.isExternal() )
        {
            return ((com.neo4j.bench.common.profiling.ExternalProfiler) innerProfiler).invokeArgs( forkDir,
                                                                                                   benchmarkGroup,
                                                                                                   benchmark,
                                                                                                   Parameters.NONE );
        }
        else
        {
            return Collections.emptyList();
        }
    }

    /**
     * NOTE: is called from parent process
     */
    @Override
    public Collection<String> addJVMOptions( BenchmarkParams params )
    {
        RunnerParams runnerParams = RunnerParams.extractFrom( params );
        BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
        Benchmark benchmark = toBenchmarks( params, runnerParams ).parentBenchmark();
        ForkDirectory forkDir = getOrCreateForkDir( runnerParams, benchmarkGroup, benchmark );
        try ( Resources resources = new Resources( runnerParams.workDir() ) )
        {
            if ( profilerType.isExternal() )
            {
                Jvm jvm = Jvm.bestEffortOrFail( Paths.get( params.getJvm() ) );
                JvmVersion jvmVersion = jvm.version();
                return ((com.neo4j.bench.common.profiling.ExternalProfiler) innerProfiler)
                        .jvmArgs(
                                jvmVersion,
                                forkDir,
                                benchmarkGroup,
                                benchmark,
                                Parameters.NONE,
                                resources )
                        .toArgs();
            }
            else
            {
                return Collections.emptyList();
            }
        }
    }

    /**
     * NOTE: is called from parent process
     */
    @Override
    public void beforeTrial( BenchmarkParams benchmarkParams )
    {
        RunnerParams runnerParams = RunnerParams.extractFrom( benchmarkParams );
        BenchmarkGroup benchmarkGroup = toBenchmarkGroup( benchmarkParams );
        Benchmark benchmark = extractBenchmark( benchmarkParams );
        ForkDirectory forkDir = getOrFailForkDir( runnerParams, benchmarkParams, benchmarkGroup, benchmark );
        if ( profilerType.isExternal() )
        {
            ((com.neo4j.bench.common.profiling.ExternalProfiler) innerProfiler).beforeProcess( forkDir, benchmarkGroup, benchmark, Parameters.NONE );
        }
    }

    /**
     * NOTE: is called from parent process
     */
    @Override
    public Collection<? extends Result> afterTrial( BenchmarkResult benchmarkResult, long pid, File stdOut, File stdErr )
    {
        BenchmarkParams benchmarkParams = benchmarkResult.getParams();
        RunnerParams runnerParams = RunnerParams.extractFrom( benchmarkParams );
        BenchmarkGroup benchmarkGroup = toBenchmarkGroup( benchmarkParams );
        Benchmark benchmark = extractBenchmark( benchmarkParams );
        ForkDirectory forkDir = getOrFailForkDir( runnerParams, benchmarkParams, benchmarkGroup, benchmark );
        if ( profilerType.isExternal() )
        {
            ((com.neo4j.bench.common.profiling.ExternalProfiler) innerProfiler).afterProcess( forkDir, benchmarkGroup, benchmark, Parameters.NONE );
            forkDir.unsanitizeProfilerRecordingsFor( benchmarkGroup, benchmark, profilerType, Parameters.NONE );
        }
        /**
         * JMH processes keep {@link org.openjdk.jmh.profile.Profiler} instances around for as long as they can.
         * This means the JMH parent process reuses the same {@link ExternalProfiler} instances for all forks of a 'run'.
         * <p>
         * We do NOT want to share {@link ForkDirectory} instances between different forks, even different forks of the same benchmark.
         * <p>
         * For that reason it is necessary to clear cached {@link forkDir} variable at the end of fork execution, here.
         */
        this.forkDir = null;
        return Collections.emptyList();
    }

    @Override
    public boolean allowPrintOut()
    {
        return true;
    }

    @Override
    public boolean allowPrintErr()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return innerProfiler.description();
    }

    @Override
    public void beforeIteration( BenchmarkParams benchmarkParams, IterationParams iterationParams )
    {
        // only start profiling on first iteration
        if ( profilerType.isInternal() && iterationParams.getType() == IterationType.MEASUREMENT && ++iteration == 1 )
        {
            RunnerParams runnerParams = RunnerParams.extractFrom( benchmarkParams );
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( benchmarkParams );
            Benchmark benchmark = extractBenchmark( benchmarkParams );
            ForkDirectory forkDir = getOrFailForkDir( runnerParams, benchmarkParams, benchmarkGroup, benchmark );
            Jvm jvm = Jvm.bestEffortOrFail( Paths.get( benchmarkParams.getJvm() ) );
            ((com.neo4j.bench.common.profiling.InternalProfiler) innerProfiler).onMeasurementBegin( jvm,
                                                                                                    forkDir,
                                                                                                    HasPid.getPid(),
                                                                                                    benchmarkGroup,
                                                                                                    benchmark,
                                                                                                    Parameters.NONE );
        }
    }

    @Override
    public Collection<? extends Result> afterIteration( BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult iterationResult )
    {
        // only stop profiling on last iteration
        if ( profilerType.isInternal() && iterationParams.getType() == IterationType.MEASUREMENT && iteration == iterationParams.getCount() )
        {
            RunnerParams runnerParams = RunnerParams.extractFrom( benchmarkParams );
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( benchmarkParams );
            Benchmark benchmark = extractBenchmark( benchmarkParams );
            ForkDirectory forkDir = getOrFailForkDir( runnerParams, benchmarkParams, benchmarkGroup, benchmark );
            Jvm jvm = Jvm.bestEffortOrFail( Paths.get( benchmarkParams.getJvm() ) );
            ((com.neo4j.bench.common.profiling.InternalProfiler) innerProfiler).onMeasurementFinished( jvm,
                                                                                                      forkDir,
                                                                                                      HasPid.getPid(),
                                                                                                      benchmarkGroup,
                                                                                                      benchmark,
                                                                                                      Parameters.NONE );
            if ( !profilerType.isExternal() )
            {
                // profiler recording cleanup should only happen once
                // if profiler is both internal and external, cleanup will happen in afterTrial()
                forkDir.unsanitizeProfilerRecordingsFor( benchmarkGroup, benchmark, profilerType, Parameters.NONE );
            }
        }
        return Collections.emptyList();
    }

    private static Benchmark extractBenchmark( BenchmarkParams params )
    {
        RunnerParams runnerParams = RunnerParams.extractFrom( params );
        return toBenchmarks( params, runnerParams ).parentBenchmark();
    }
}
