/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.ImmutableMap;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.profiling.RecordingType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProfilerRecordingDescriptorTest
{

    @Test
    public void escapeParametersInFilename()
    {
        // given
        var benchmarkGroup = new BenchmarkGroup( "benchmarkGroup" );
        var benchmark = Benchmark.benchmarkFor( "description", "simpleName", Benchmark.Mode.LATENCY, Collections.emptyMap() );
        var descriptor = ProfilerRecordingDescriptor
                .create( benchmarkGroup,
                         benchmark,
                         RunPhase.MEASUREMENT,
                         ParameterizedProfiler.defaultProfiler( ProfilerType.GC ),
                         new Parameters( ImmutableMap.of( "rel", "KNOWS | WORKS_AT" ) ) );
        // when
        var recordingDescriptor = descriptor.recordingDescriptorFor( RecordingType.GC_LOG );
        var filename = recordingDescriptor.sanitizedFilename();
        // then
        assertTrue( filename.matches( "[\\w\\d-_.%]*" ) ); //this is filename regexp accepted by JVM parameters
    }
}
