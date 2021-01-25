/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.profiling.RecordingType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ProfilerRecordingDescriptor
{
    public static ProfilerRecordingDescriptor create( BenchmarkGroup benchmarkGroup,
                                                      Benchmark benchmark,
                                                      RunPhase runPhase,
                                                      ParameterizedProfiler profiler,
                                                      Parameters additionalParams )
    {
        return create( benchmarkGroup, benchmark, runPhase, profiler, additionalParams, Collections.emptySet() );
    }

    public static ProfilerRecordingDescriptor create( BenchmarkGroup benchmarkGroup,
                                                      Benchmark benchmark,
                                                      RunPhase runPhase,
                                                      ParameterizedProfiler profiler,
                                                      Parameters additionalParams,
                                                      Set<Benchmark> secondary )
    {
        FullBenchmarkName benchmarkName = FullBenchmarkName.from( benchmarkGroup, benchmark );
        List<RecordingType> secondaryRecordingTypes = new ArrayList<>( profiler.profilerType().allRecordingTypes() );
        secondaryRecordingTypes.remove( profiler.profilerType().recordingType() );
        Set<FullBenchmarkName> secondaryBenchmarks = secondary.stream()
                                                              .map( secondaryBenchmark -> FullBenchmarkName.from( benchmarkGroup, secondaryBenchmark ) )
                                                              .collect( Collectors.toSet() );
        return new ProfilerRecordingDescriptor( benchmarkName,
                                                runPhase,
                                                profiler.profilerType().recordingType(),
                                                secondaryRecordingTypes,
                                                additionalParams,
                                                secondaryBenchmarks );
    }

    private final FullBenchmarkName benchmarkName;
    private final RunPhase runPhase;
    private final RecordingType recordingType;
    private final List<RecordingType> secondaryRecordingTypes;
    private final Parameters additionalParams;
    private final Set<FullBenchmarkName> secondaryBenchmarks;

    private ProfilerRecordingDescriptor( FullBenchmarkName benchmarkName,
                                         RunPhase runPhase,
                                         RecordingType recordingType,
                                         List<RecordingType> secondaryRecordingTypes,
                                         Parameters additionalParams,
                                         Set<FullBenchmarkName> secondaryBenchmarks )
    {
        this.benchmarkName = benchmarkName;
        this.runPhase = runPhase;
        this.recordingType = recordingType;
        this.secondaryRecordingTypes = secondaryRecordingTypes;
        this.additionalParams = additionalParams;
        this.secondaryBenchmarks = secondaryBenchmarks;
    }

    public RecordingDescriptor recordingDescriptorFor( RecordingType aRecordingType )
    {
        if ( !isSupportedRecordingType( aRecordingType ) )
        {
            throw new RuntimeException( "Invalid recording type: " + aRecordingType + "\n" +
                                        "Valid types: " + recordingTypes() );
        }
        return new RecordingDescriptor( benchmarkName, runPhase, aRecordingType, additionalParams, secondaryBenchmarks, false );
    }

    private List<RecordingType> recordingTypes()
    {
        List<RecordingType> validRecordingTypes = new ArrayList<>( secondaryRecordingTypes );
        validRecordingTypes.add( recordingType );
        return validRecordingTypes;
    }

    private boolean isSupportedRecordingType( RecordingType recordingType )
    {
        return this.recordingType == recordingType || secondaryRecordingTypes.contains( recordingType );
    }

    @Override
    public String toString()
    {
        return String.format( "%s\n" +
                              "  %s\n" +
                              "  %s\n" +
                              "  %s:%s\n" +
                              "  %s",
                              getClass().getSimpleName(),
                              benchmarkName,
                              runPhase,
                              recordingType,
                              secondaryRecordingTypes,
                              additionalParams );
    }
}
