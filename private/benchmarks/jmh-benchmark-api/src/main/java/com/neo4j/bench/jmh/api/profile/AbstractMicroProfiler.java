/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.profile;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.HasPid;
import com.neo4j.bench.common.profiling.Profiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.jmh.api.config.RunnerParams;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.ExternalProfiler;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.runner.IterationType;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

import static com.neo4j.bench.jmh.api.BenchmarkDiscoveryUtils.toBenchmarkGroup;
import static com.neo4j.bench.jmh.api.BenchmarkDiscoveryUtils.toBenchmarks;
import static java.util.Collections.singletonList;

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

    private ForkDirectory getForkDir( BenchmarkParams params, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        if ( null == forkDir )
        {
            forkDir = findOrCreateForkDirectoryFor( benchmarkGroup, benchmark, params );
        }
        return forkDir;
    }

    private ForkDirectory findOrCreateForkDirectoryFor( BenchmarkGroup benchmarkGroup,
                                                        Benchmark benchmark,
                                                        BenchmarkParams benchmarkParams )
    {
        Path workDir = RunnerParams.extractFrom( benchmarkParams ).workDir();
        BenchmarkGroupDirectory benchmarkGroupDir = BenchmarkGroupDirectory.findOrCreateAt( workDir, benchmarkGroup );
        BenchmarkDirectory benchmarkDir = benchmarkGroupDir.findOrCreate( benchmark );
        String forkName = "fork_" + profilerType.name();
        return benchmarkDir.findOrCreate( forkName, singletonList( profilerType ) );
    }

    @Override
    public Collection<String> addJVMInvokeOptions( BenchmarkParams params )
    {
        if ( profilerType.isExternal() )
        {
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = extractBenchmark( params );
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
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

    @Override
    public Collection<String> addJVMOptions( BenchmarkParams params )
    {
        RunnerParams runnerParams = RunnerParams.extractFrom( params );
        try ( Resources resources = new Resources( runnerParams.workDir() ) )
        {
            if ( profilerType.isExternal() )
            {
                BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
                Benchmark benchmark = toBenchmarks( params, runnerParams ).parentBenchmark();
                ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
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

    @Override
    public void beforeTrial( BenchmarkParams params )
    {
        if ( profilerType.isExternal() )
        {
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = extractBenchmark( params );
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            ((com.neo4j.bench.common.profiling.ExternalProfiler) innerProfiler).beforeProcess( forkDir, benchmarkGroup, benchmark, Parameters.NONE );
        }
    }

    @Override
    public Collection<? extends Result> afterTrial( BenchmarkResult benchmarkResult, long pid, File stdOut, File stdErr )
    {
        if ( profilerType.isExternal() )
        {
            BenchmarkParams params = benchmarkResult.getParams();
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = extractBenchmark( params );
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            ((com.neo4j.bench.common.profiling.ExternalProfiler) innerProfiler).afterProcess( forkDir, benchmarkGroup, benchmark, Parameters.NONE );
            forkDir.unsanitizeProfilerRecordingsFor( benchmarkGroup, benchmark, profilerType, Parameters.NONE );
        }
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
    public void beforeIteration( BenchmarkParams params, IterationParams iterationParams )
    {
        // only start profiling on first iteration
        if ( profilerType.isInternal() && iterationParams.getType() == IterationType.MEASUREMENT && ++iteration == 1 )
        {
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = extractBenchmark( params );
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            Jvm jvm = Jvm.bestEffortOrFail( Paths.get( params.getJvm() ) );
            ((com.neo4j.bench.common.profiling.InternalProfiler) innerProfiler).onMeasurementBegin( jvm,
                                                                                                    forkDir,
                                                                                                    HasPid.getPid(),
                                                                                                    benchmarkGroup,
                                                                                                    benchmark,
                                                                                                    Parameters.NONE );
        }
    }

    @Override
    public Collection<? extends Result> afterIteration( BenchmarkParams params, IterationParams iterationParams, IterationResult iterationResult )
    {
        // only stop profiling on last iteration
        if ( profilerType.isInternal() && iterationParams.getType() == IterationType.MEASUREMENT && iteration == iterationParams.getCount() )
        {
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = extractBenchmark( params );
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            Jvm jvm = Jvm.bestEffortOrFail( Paths.get( params.getJvm() ) );
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
