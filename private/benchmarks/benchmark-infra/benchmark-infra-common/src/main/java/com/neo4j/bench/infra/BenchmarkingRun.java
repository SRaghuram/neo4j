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
public class BenchmarkingRun<P>
{

    private final BenchmarkingTool<P> benchmarkingTool;
    private final String testRunId;

    @JsonCreator
    public BenchmarkingRun( @JsonProperty( "benchmarkingTool" ) BenchmarkingTool<P> benchmarkingTool,
                            @JsonProperty( "testRunId" ) String testRunId )
    {
        this.benchmarkingTool = Objects.requireNonNull( benchmarkingTool );
        this.testRunId = testRunId;
    }

    public BenchmarkingTool<P> benchmarkingTool()
    {
        return benchmarkingTool;
    }

    public String testRunId()
    {
        return testRunId;
    }

    @Override
    public boolean equals( Object that )
    {
        return EqualsBuilder.reflectionEquals( this, that );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }
}
