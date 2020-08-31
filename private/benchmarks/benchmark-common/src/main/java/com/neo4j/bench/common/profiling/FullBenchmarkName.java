/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static com.neo4j.bench.common.util.BenchmarkUtil.sanitize;

public class FullBenchmarkName
{
    public static FullBenchmarkName from( BenchmarkGroupBenchmark benchmarkGroupBenchmark )
    {
        return from( benchmarkGroupBenchmark.benchmarkGroup(), benchmarkGroupBenchmark.benchmark() );
    }

    public static FullBenchmarkName from( BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return new FullBenchmarkName( benchmarkGroup, benchmark );
    }

    private final BenchmarkGroup benchmarkGroup;
    private final Benchmark benchmark;

    @JsonCreator
    private FullBenchmarkName( @JsonProperty( "benchmarkGroup" ) BenchmarkGroup benchmarkGroup,
                               @JsonProperty( "benchmark" ) Benchmark benchmark )
    {
        this.benchmarkGroup = benchmarkGroup;
        this.benchmark = benchmark;
    }

    public String sanitizedName()
    {
        return sanitize( name() );
    }

    public String name()
    {
        return benchmarkGroup.name() + "." + benchmark.name();
    }

    @Override
    public String toString()
    {
        return name();
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
}
