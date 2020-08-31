/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.lang.String.format;

public class BenchmarkGroupBenchmark
{
    private final BenchmarkGroup benchmarkGroup;
    private final Benchmark benchmark;

    @JsonCreator
    public BenchmarkGroupBenchmark( @JsonProperty( "benchmarkGroup" ) BenchmarkGroup benchmarkGroup,
                                    @JsonProperty( "benchmark" ) Benchmark benchmark )
    {
        this.benchmarkGroup = benchmarkGroup;
        this.benchmark = benchmark;
    }

    public BenchmarkGroup benchmarkGroup()
    {
        return benchmarkGroup;
    }

    public Benchmark benchmark()
    {
        return benchmark;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        BenchmarkGroupBenchmark that = (BenchmarkGroupBenchmark) o;
        return Objects.equals( benchmarkGroup, that.benchmarkGroup ) &&
               Objects.equals( benchmark, that.benchmark );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( benchmarkGroup, benchmark );
    }

    @Override
    public String toString()
    {
        return format( "(%s , %s)", benchmarkGroup.name(), benchmark.name() );
    }
}
