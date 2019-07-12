/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.model;

import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.RecordingType;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.BenchmarkUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.neo4j.bench.common.model.Benchmark.Mode.LATENCY;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ProfilerRecordingsTest
{
    @Test
    public void shouldWorkInRegularCase()
    {
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "group" );
        Benchmark benchmark = Benchmark.benchmarkFor( "description", "simple_name", LATENCY, singletonMap( "k", "v" ) );

        // empty map
        List<Map<String,String>> parametersList = new ArrayList<>();
        Map<String,String> mutableParameters = new HashMap<>();
        parametersList.add( new HashMap<>( mutableParameters ) );

        mutableParameters.put( "k1", "v2" );
        // single parameter map
        parametersList.add( new HashMap<>( mutableParameters ) );

        mutableParameters.put( "k2", "v2" );
        // double parameter map
        parametersList.add( new HashMap<>( mutableParameters ) );

        mutableParameters.put( "k_3", "v_3" );
        // triple parameter map
        parametersList.add( new HashMap<>( mutableParameters ) );

        ProfilerRecordings profilerRecordings = new ProfilerRecordings();
        int expectedSize = 0;

        for ( ProfilerType profilerType : ProfilerType.values() )
        {
            for ( RecordingType recordingType : profilerType.allRecordingTypes() )
            {
                for ( Map<String,String> parametersMap : parametersList )
                {
                    Parameters parameters = Parameters.fromMap( parametersMap );
                    String noParamsFilename = ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                                                  benchmark,
                                                                                  RunPhase.MEASUREMENT,
                                                                                  profilerType,
                                                                                  parameters ).sanitizedFilename( recordingType );
                    profilerRecordings.with( recordingType, parameters, "other-s3-bucket/" + noParamsFilename );
                    expectedSize++;
                    assertThat( profilerRecordings.toMap().size(), equalTo( expectedSize ) );
                }
            }
        }
    }

    @Test
    public void shouldDisallowDuplicateEntriesForSameBenchmarkAndParameters()
    {
        ProfilerRecordings profilerRecordings = new ProfilerRecordings();
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "group" );
        Benchmark benchmark = Benchmark.benchmarkFor( "description", "simple_name", LATENCY, singletonMap( "k", "v" ) );
        RecordingType recordingType = RecordingType.JFR_FLAMEGRAPH;

        String filename = ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                              benchmark,
                                                              RunPhase.MEASUREMENT,
                                                              ProfilerType.JFR,
                                                              Parameters.CLIENT ).sanitizedFilename( recordingType );

        // add client process recording
        profilerRecordings.with( recordingType, Parameters.CLIENT, "other-s3-bucket/" + filename );

        // add client process recording again -- should crash
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> profilerRecordings.with( recordingType, Parameters.CLIENT, "other-s3-bucket/" + filename ) );
    }

    @Test
    public void shouldDisallowPoorlyFormedPath()
    {
        ProfilerRecordings profilerRecordings = new ProfilerRecordings();

        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> profilerRecordings.with( RecordingType.JFR_FLAMEGRAPH, Parameters.CLIENT, "path_with_no_folder_prefix" ) );
    }
}
