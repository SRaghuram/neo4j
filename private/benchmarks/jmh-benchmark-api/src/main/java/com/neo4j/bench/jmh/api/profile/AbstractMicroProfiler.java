/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.profile;

import com.neo4j.bench.common.process.HasPid;
import com.neo4j.bench.common.profiling.Profiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.assist.ExternalProfilerAssist;
import com.neo4j.bench.common.profiling.assist.InternalProfilerAssist;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.Benchmarks;
import com.neo4j.bench.jmh.api.JmhLifecycleTracker;
import com.neo4j.bench.jmh.api.RunnerParams;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
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
import java.util.List;

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
        case NO_OP:
            return NoOpProfiler.class;
        case VM_STAT:
            return VmStatTracer.class;
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
                                            Benchmarks benchmarks )
    {
        if ( null == forkDir )
        {
            JmhLifecycleTracker jmhLifecycleTracker = JmhLifecycleTracker.load( runnerParams.workDir() );
            boolean isForking = benchmarkParams.getForks() > 0;
            forkDir = jmhLifecycleTracker.getForkDirectory( runnerParams, isForking, benchmarkGroup, benchmarks.parentBenchmark() );
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
        if ( profilerType.isExternal() )
        {
            return externalProfilerAssist( params, runnerParams ).invokeArgs();
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
        if ( profilerType.isExternal() )
        {
            JvmArgs jvmArgs = externalProfilerAssist( params, runnerParams ).jvmArgs();
            return jvmArgs.toArgs();
        }
        else
        {
            return Collections.emptyList();
        }
    }

    private ExternalProfilerAssist externalProfilerAssist( BenchmarkParams params, RunnerParams runnerParams )
    {
        BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
        Benchmarks benchmarks = toBenchmarks( params, runnerParams );
        Benchmark benchmark = benchmarks.parentBenchmark();
        ForkDirectory forkDir = getOrCreateForkDir( runnerParams, benchmarkGroup, benchmark );
        return externalProfilerAssist( benchmarkGroup, benchmarks, forkDir, params );
    }

    /**
     * NOTE: is called from parent process
     */
    @Override
    public void beforeTrial( BenchmarkParams benchmarkParams )
    {
        if ( profilerType.isExternal() )
        {
            externalProfilerAssist( benchmarkParams ).beforeProcess();
        }
    }

    /**
     * NOTE: is called from parent process
     */
    @Override
    public Collection<? extends Result> afterTrial( BenchmarkResult benchmarkResult, long pid, File stdOut, File stdErr )
    {
        BenchmarkParams benchmarkParams = benchmarkResult.getParams();
        if ( profilerType.isExternal() )
        {
            externalProfilerAssist( benchmarkParams ).afterProcess();
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

    private ExternalProfilerAssist externalProfilerAssist( BenchmarkParams benchmarkParams )
    {
        RunnerParams runnerParams = RunnerParams.extractFrom( benchmarkParams );
        BenchmarkGroup benchmarkGroup = toBenchmarkGroup( benchmarkParams );
        Benchmarks benchmarks = toBenchmarks( benchmarkParams, runnerParams );
        ForkDirectory forkDir = getOrFailForkDir( runnerParams, benchmarkParams, benchmarkGroup, benchmarks );
        return externalProfilerAssist( benchmarkGroup, benchmarks, forkDir, benchmarkParams );
    }

    private ExternalProfilerAssist externalProfilerAssist( BenchmarkGroup benchmarkGroup, Benchmarks benchmarks, ForkDirectory forkDir, BenchmarkParams params )
    {
        List<com.neo4j.bench.common.profiling.ExternalProfiler> profilers =
                Collections.singletonList( (com.neo4j.bench.common.profiling.ExternalProfiler) innerProfiler );
        Jvm jvm = Jvm.bestEffortOrFail( Paths.get( params.getJvm() ) );
        return ExternalProfilerAssist.create( profilers,
                                              forkDir,
                                              benchmarkGroup,
                                              benchmarks.parentBenchmark(),
                                              benchmarks.childBenchmarks(),
                                              jvm,
                                              Parameters.NONE );
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
            internalProfilerAssist( benchmarkParams ).onMeasurementBegin();
        }
    }

    @Override
    public Collection<? extends Result> afterIteration( BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult iterationResult )
    {
        // only stop profiling on last iteration
        if ( profilerType.isInternal() && iterationParams.getType() == IterationType.MEASUREMENT && iteration == iterationParams.getCount() )
        {
            internalProfilerAssist( benchmarkParams ).onMeasurementFinished();
        }
        return Collections.emptyList();
    }

    private InternalProfilerAssist internalProfilerAssist( BenchmarkParams benchmarkParams )
    {
        List<com.neo4j.bench.common.profiling.InternalProfiler> profilers =
                Collections.singletonList( (com.neo4j.bench.common.profiling.InternalProfiler) innerProfiler );
        RunnerParams runnerParams = RunnerParams.extractFrom( benchmarkParams );
        BenchmarkGroup benchmarkGroup = toBenchmarkGroup( benchmarkParams );
        Benchmarks benchmarks = toBenchmarks( benchmarkParams, runnerParams );
        ForkDirectory forkDir = getOrFailForkDir( runnerParams, benchmarkParams, benchmarkGroup, benchmarks );
        Jvm jvm = Jvm.bestEffortOrFail( Paths.get( benchmarkParams.getJvm() ) );
        return InternalProfilerAssist.forProcess( profilers,
                                                  forkDir,
                                                  benchmarkGroup,
                                                  benchmarks.parentBenchmark(),
                                                  benchmarks.childBenchmarks(),
                                                  jvm,
                                                  HasPid.getPid() );
    }
}
