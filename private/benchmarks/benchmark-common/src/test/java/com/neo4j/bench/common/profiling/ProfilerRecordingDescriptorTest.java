/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.ImmutableMap;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class ProfilerRecordingDescriptorTest
{

    @Test
    public void escapeParametersInFilename()
    {
        // given
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "benchmarkGroup" );
        Benchmark benchmark = Benchmark.benchmarkFor( "description", "simpleName", Benchmark.Mode.LATENCY, Collections.emptyMap() );
        ProfilerRecordingDescriptor descriptor = ProfilerRecordingDescriptor
                .create( benchmarkGroup,
                         benchmark,
                         RunPhase.MEASUREMENT,
                         ProfilerType.GC,
                         new Parameters( ImmutableMap.of( "rel", "KNOWS | WORKS_AT" ) ) );
        // when
        String filename = descriptor.sanitizedFilename();
        // then
        assertTrue( filename.matches( "[\\w\\d-_.%]*" ) ); //this is filename regexp accepted by JVM parameters
    }
}
