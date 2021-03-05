/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.assist;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ScheduledProfiler;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface ExternalProfilerAssist
{

    static ExternalProfilerAssist create( List<ExternalProfiler> profilers,
                                          ForkDirectory forkDirectory,
                                          BenchmarkGroup benchmarkGroup,
                                          Benchmark benchmark,
                                          Set<Benchmark> secondary,
                                          Jvm jvm,
                                          Parameters parameters )

    {
        List<ExternalProfilerAssist> collect = profilers.stream()
                                                        .map( profiler -> create( profiler,
                                                                                  forkDirectory,
                                                                                  benchmarkGroup,
                                                                                  benchmark,
                                                                                  secondary,
                                                                                  jvm,
                                                                                  parameters
                                                        ) )
                                                        .collect( Collectors.toList() );
        return new CompositeExternalProfilerAssist( collect );
    }

    static ExternalProfilerAssist create( ExternalProfiler profiler,
                                          ForkDirectory forkDirectory,
                                          BenchmarkGroup benchmarkGroup,
                                          Benchmark benchmark,
                                          Set<Benchmark> secondary,
                                          Jvm jvm,
                                          Parameters parameters )
    {
        ProfilerRecordingDescriptor descriptor = BenchmarkExternalProfilerAssist.descriptor( profiler,
                                                                                             benchmarkGroup,
                                                                                             benchmark,
                                                                                             parameters,
                                                                                             secondary );
        ExternalProfilerAssist baseAssist = new BenchmarkExternalProfilerAssist( profiler,
                                                                                 forkDirectory,
                                                                                 jvm.version(),
                                                                                 descriptor );
        if ( profiler instanceof ScheduledProfiler )
        {
            return new ScheduledExternalProfilerAssist( (ScheduledProfiler) profiler,
                                                        baseAssist,
                                                        forkDirectory,
                                                        descriptor,
                                                        jvm );
        }
        else
        {
            return baseAssist;
        }
    }

    List<String> invokeArgs();

    JvmArgs jvmArgs();

    // TODO filter out OOMProfiler for server, as we don't support spaces in additional jmv args
    // https://trello.com/c/NF6by0ki/5084-dbmsjvmadditional-with-spaces-in-them
    JvmArgs jvmArgsWithoutOOM();

    void beforeProcess();

    void afterProcess();

    void processFailed();

    void schedule( Pid pid );
}
