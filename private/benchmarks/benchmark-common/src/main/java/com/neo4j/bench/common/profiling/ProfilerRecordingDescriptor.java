/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.List;

public class ProfilerRecordingDescriptor
{
    public static ProfilerRecordingDescriptor create( BenchmarkGroup benchmarkGroup,
                                                      Benchmark benchmark,
                                                      RunPhase runPhase,
                                                      ParameterizedProfiler profiler,
                                                      Parameters additionalParams )
    {
        List<RecordingType> secondaryRecordingTypes = new ArrayList<>( profiler.profilerType().allRecordingTypes() );
        secondaryRecordingTypes.remove( profiler.profilerType().recordingType() );
        return new ProfilerRecordingDescriptor( benchmarkGroup,
                                                benchmark,
                                                runPhase,
                                                profiler.profilerType().recordingType(),
                                                secondaryRecordingTypes,
                                                additionalParams );
    }

    private final FullBenchmarkName benchmarkName;
    private final RunPhase runPhase;
    private final RecordingType recordingType;
    private final List<RecordingType> secondaryRecordingTypes;
    private final Parameters additionalParams;

    public ProfilerRecordingDescriptor( BenchmarkGroup benchmarkGroup,
                                        Benchmark benchmark,
                                        RunPhase runPhase,
                                        RecordingType recordingType,
                                        List<RecordingType> secondaryRecordingTypes,
                                        Parameters additionalParams )
    {
        this.benchmarkName = FullBenchmarkName.from( benchmarkGroup, benchmark );
        this.runPhase = runPhase;
        this.recordingType = recordingType;
        this.secondaryRecordingTypes = secondaryRecordingTypes;
        this.additionalParams = additionalParams;
    }

    public RecordingDescriptor recordingDescriptorFor( RecordingType aRecordingType )
    {
        if ( !isSupportedRecordingType( aRecordingType ) )
        {
            throw new RuntimeException( "Invalid recording type: " + aRecordingType + "\n" +
                                        "Valid types: " + recordingTypes() );
        }
        return new RecordingDescriptor( benchmarkName, runPhase, aRecordingType, additionalParams );
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
