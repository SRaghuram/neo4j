/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestRunErrorTest
{

    @Test
    public void toMap()
    {
        // given
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "group" );
        Benchmark benchmark =
                Benchmark.benchmarkFor( "description",
                                        "simpleName",
                                        Benchmark.Mode.LATENCY,
                                        ImmutableMap.of( "param1", "value1" ),
                                        "Q1" );

        TestRunError error = new TestRunError( benchmarkGroup, benchmark, "error" );
        // when
        Map<String,Object> map = error.toMap();
        // then
        assertEquals( map, ImmutableMap.<String,Object>of(
                "benchmarkGroupName", "group",
                "benchmarkProperties", ImmutableMap.<String,Object>builder()
                        .put( "mode", "LATENCY" )
                        .put( "cypher_query", "Q1" )
                        .put( "name", "simpleName_(param1,value1)_(mode,LATENCY)" )
                        .put( "simple_name", "simpleName" )
                        .put( "description", "description" )
                        .put( "active", Boolean.TRUE )
                        .build()
                , "benchmarkParams", ImmutableMap.of(
                        "param1", "value1"
                )
                , "message", "error"
        ) );
    }
}
