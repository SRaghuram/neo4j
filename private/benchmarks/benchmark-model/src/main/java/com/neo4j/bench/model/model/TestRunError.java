/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestRunError
{
    public static final String BENCHMARK_GROUP_NAME = "benchmarkGroupName";
    public static final String BENCHMARK_PROPERTIES = "benchmarkProperties";
    public static final String BENCHMARK_PARAMS = "benchmarkParams";
    public static final String MESSAGE = "message";

    private final BenchmarkGroup benchmarkGroup;
    private final Benchmark benchmark;
    private final String message;

    @JsonCreator
    public TestRunError( @JsonProperty( "benchmarkGroup" ) BenchmarkGroup benchmarkGroup,
                         @JsonProperty( "benchmark" ) Benchmark benchmark,
                         @JsonProperty( "message" ) String message )
    {
        this.benchmarkGroup = requireNonNull( benchmarkGroup );
        this.benchmark = requireNonNull( benchmark );
        this.message = message;
    }

    public BenchmarkGroup benchmarkGroup()
    {
        return benchmarkGroup;
    }

    public Benchmark benchmark()
    {
        return benchmark;
    }

    public String message()
    {
        return message;
    }

    public Map<String,Object> toMap()
    {
        Map<String,Object> map = new HashMap<>();
        map.put( BENCHMARK_GROUP_NAME, benchmarkGroup.name() );
        map.put( BENCHMARK_PROPERTIES, benchmark.toMap() );
        map.put( BENCHMARK_PARAMS, benchmark.parameters() );
        map.put( MESSAGE, message );
        return map;
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return format( "%s.%s\n" +
                       "%s", benchmarkGroup, benchmark, message );
    }
}
