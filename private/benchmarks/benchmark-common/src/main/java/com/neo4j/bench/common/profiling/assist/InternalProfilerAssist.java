/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.assist;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface InternalProfilerAssist
{

    static InternalProfilerAssist forProcess( List<InternalProfiler> profilers,
                                              ForkDirectory forkDir,
                                              BenchmarkGroup benchmarkGroup,
                                              Benchmark benchmark,
                                              Set<Benchmark> secondary,
                                              Jvm jvm,
                                              Pid pid )
    {
        ProfilerPidMapping pidMapping = new ProfilerPidMapping( pid, Parameters.NONE, profilers );
        ProfilerPidMappings mappings = new ProfilerPidMappings( Parameters.NONE, Collections.singletonList( pidMapping ) );
        return fromMapping( mappings, forkDir, benchmarkGroup, benchmark, secondary, jvm );
    }

    static InternalProfilerAssist fromMapping( ProfilerPidMappings mappings,
                                               ForkDirectory forkDirectory,
                                               BenchmarkGroup benchmarkGroup,
                                               Benchmark benchmark,
                                               Set<Benchmark> secondary,
                                               Jvm jvm )
    {
        Parameters ownParameters = mappings.ownParameters();
        List<InternalProfilerAssist> assists = mappings.profilerPidMappings()
                                                       .stream()
                                                       .flatMap( mapping ->
                                                                 {
                                                                     Parameters parameters = mapping.parameters();
                                                                     Pid pid = mapping.pid();
                                                                     return mapping.profilers()
                                                                                   .stream()
                                                                                   .map( profile -> BenchmarkInternalProfilerAssist.create( ownParameters,
                                                                                                                                            profile,
                                                                                                                                            forkDirectory,
                                                                                                                                            benchmarkGroup,
                                                                                                                                            benchmark,
                                                                                                                                            secondary,
                                                                                                                                            jvm,
                                                                                                                                            parameters,
                                                                                                                                            pid ) );
                                                                 } )
                                                       .collect( Collectors.toList() );
        return new CompositeInternalProfilerAssist( ownParameters, assists );
    }

    Parameters ownParameter();

    void onWarmupBegin();

    void onWarmupFinished();

    void onMeasurementBegin();

    void onMeasurementFinished();
}
