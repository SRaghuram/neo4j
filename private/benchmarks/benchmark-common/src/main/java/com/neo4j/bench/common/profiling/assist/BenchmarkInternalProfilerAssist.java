/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.assist;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.profiling.Profiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;

import java.util.Set;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfiler;

class BenchmarkInternalProfilerAssist implements InternalProfilerAssist
{

    private final Parameters ownParameters;
    private final InternalProfiler profiler;
    private final ForkDirectory forkDirectory;
    private final Jvm jvm;
    private final Pid pid;
    private final ProfilerRecordingDescriptor measurementDescriptor;
    private final ProfilerRecordingDescriptor warmupDescriptor;

    public static BenchmarkInternalProfilerAssist create( Parameters ownParameters,
                                                          InternalProfiler profiler,
                                                          ForkDirectory forkDirectory,
                                                          BenchmarkGroup benchmarkGroup,
                                                          Benchmark benchmark,
                                                          Set<Benchmark> secondary,
                                                          Jvm jvm,
                                                          Parameters parameters,
                                                          Pid pid )
    {
        return new BenchmarkInternalProfilerAssist( ownParameters,
                                                    profiler,
                                                    forkDirectory,
                                                    jvm,
                                                    pid,
                                                    descriptor( profiler, benchmarkGroup, benchmark, secondary, parameters, RunPhase.MEASUREMENT ),
                                                    descriptor( profiler, benchmarkGroup, benchmark, secondary, parameters, RunPhase.WARMUP ) );
    }

    BenchmarkInternalProfilerAssist( Parameters ownParameters,
                                     InternalProfiler profiler,
                                     ForkDirectory forkDirectory,
                                     Jvm jvm,
                                     Pid pid,
                                     ProfilerRecordingDescriptor measurementDescriptor,
                                     ProfilerRecordingDescriptor warmupDescriptor )
    {
        this.ownParameters = ownParameters;
        this.profiler = profiler;
        this.forkDirectory = forkDirectory;
        this.jvm = jvm;
        this.pid = pid;
        this.measurementDescriptor = measurementDescriptor;
        this.warmupDescriptor = warmupDescriptor;
    }

    private static ProfilerRecordingDescriptor descriptor( Profiler profiler,
                                                           BenchmarkGroup benchmarkGroup,
                                                           Benchmark benchmark,
                                                           Set<Benchmark> secondary,
                                                           Parameters parameters,
                                                           RunPhase runPhase )
    {
        return ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                   benchmark,
                                                   runPhase,
                                                   defaultProfiler( ProfilerType.typeOf( profiler ) ),
                                                   parameters,
                                                   secondary );
    }

    @Override
    public Parameters ownParameter()
    {
        return ownParameters;
    }

    @Override
    public void onWarmupBegin()
    {
        profiler.onWarmupBegin( jvm, forkDirectory, pid, warmupDescriptor );
    }

    @Override
    public void onWarmupFinished()
    {
        profiler.onWarmupFinished( jvm, forkDirectory, pid, warmupDescriptor );
    }

    @Override
    public void onMeasurementBegin()
    {
        profiler.onMeasurementBegin( jvm, forkDirectory, pid, measurementDescriptor );
    }

    @Override
    public void onMeasurementFinished()
    {
        profiler.onMeasurementFinished( jvm, forkDirectory, pid, measurementDescriptor );
    }
}
