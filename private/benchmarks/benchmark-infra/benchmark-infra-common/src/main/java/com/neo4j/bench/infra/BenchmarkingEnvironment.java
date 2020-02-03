/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Objects;

/**
 * Describes environment in which we run benchmark.
 */
public class BenchmarkingEnvironment
{

    private final BenchmarkingTool benchmarkingTool;

    @JsonCreator
    public BenchmarkingEnvironment(
            @JsonProperty( "benchmarkingTool" ) BenchmarkingTool benchmarkingTool )
    {
        Objects.requireNonNull( benchmarkingTool );
        this.benchmarkingTool = benchmarkingTool;
    }

    /**
     * @return benchmarking tool, which will be run in this benchmarking environment,
     * like micro or macro benchmarks
     */
    public BenchmarkingTool benchmarkingTool()
    {
        return benchmarkingTool;
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
