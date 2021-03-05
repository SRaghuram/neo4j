/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.assist;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.OOMProfiler;
import com.neo4j.bench.common.profiling.Profiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;

import java.util.List;
import java.util.Set;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfiler;

class BenchmarkExternalProfilerAssist implements ExternalProfilerAssist
{
    private final ExternalProfiler profiler;
    private final ForkDirectory forkDirectory;
    private final JvmVersion jvmVersion;
    private final ProfilerRecordingDescriptor descriptor;

    static ProfilerRecordingDescriptor descriptor( Profiler profiler,
                                                   BenchmarkGroup benchmarkGroup,
                                                   Benchmark benchmark,
                                                   Parameters parameters,
                                                   Set<Benchmark> secondary )
    {
        return ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                   benchmark,
                                                   RunPhase.MEASUREMENT,
                                                   defaultProfiler( ProfilerType.typeOf( profiler ) ),
                                                   parameters,
                                                   secondary );
    }

    BenchmarkExternalProfilerAssist( ExternalProfiler profiler,
                                     ForkDirectory forkDirectory,
                                     JvmVersion jvmVersion,
                                     ProfilerRecordingDescriptor descriptor )
    {
        this.profiler = profiler;
        this.forkDirectory = forkDirectory;
        this.jvmVersion = jvmVersion;
        this.descriptor = descriptor;
    }

    @Override
    public List<String> invokeArgs()
    {
        return profiler.invokeArgs( forkDirectory, descriptor );
    }

    @Override
    public JvmArgs jvmArgsWithoutOOM()
    {
        if ( profiler instanceof OOMProfiler )
        {
            return JvmArgs.empty();
        }
        return jvmArgs();
    }

    @Override
    public JvmArgs jvmArgs()
    {
        return profiler.jvmArgs( jvmVersion, forkDirectory, descriptor );
    }

    @Override
    public void beforeProcess()
    {
        profiler.beforeProcess( forkDirectory, descriptor );
    }

    @Override
    public void afterProcess()
    {
        profiler.afterProcess( forkDirectory, descriptor );
    }

    @Override
    public void processFailed()
    {
        profiler.processFailed( forkDirectory, descriptor );
    }

    @Override
    public void schedule( Pid pid )
    {
    }
}
