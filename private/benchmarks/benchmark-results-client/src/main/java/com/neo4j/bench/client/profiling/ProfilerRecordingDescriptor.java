/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.results.RunPhase;

import static com.neo4j.bench.client.util.BenchmarkUtil.sanitize;

public class ProfilerRecordingDescriptor
{
    private final FullBenchmarkName benchmarkName;
    private final RunPhase runPhase;
    private final ProfilerType profiler;

    public ProfilerRecordingDescriptor( BenchmarkGroup benchmarkGroup,
                                        Benchmark benchmark,
                                        RunPhase runPhase,
                                        ProfilerType profiler )
    {
        this.benchmarkName = FullBenchmarkName.from( benchmarkGroup, benchmark );
        this.runPhase = runPhase;
        this.profiler = profiler;
    }

    public String filename()
    {
        return filename( profiler.recordingType() );
    }

    public String filename( RecordingType recordingType )
    {
        if ( !profiler.isSupportedRecordingType( recordingType ) )
        {
            throw new RuntimeException( "Invalid recording type for profiler\n" +
                                        "  * Profiler       : " + profiler + "\n" +
                                        "  * Recording Type : " + recordingType );
        }
        return name() + recordingType.extension();
    }

    public String name()
    {
        String name = benchmarkName.sanitizedName() + phaseModifier( runPhase );
        return sanitize( name );
    }

    public ProfilerType profiler()
    {
        return profiler;
    }

    private static String phaseModifier( RunPhase runPhase )
    {
        switch ( runPhase )
        {
        case WARMUP:
            return "_WARMUP";
        case MEASUREMENT:
            return "";
        default:
            throw new RuntimeException( "Unrecognized run phase: " + runPhase );
        }
    }
}
