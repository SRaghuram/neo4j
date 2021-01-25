/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class JobParams<P>
{

    private final InfraParams infraParams;
    private final BenchmarkingRun<P> benchmarkingRun;

    @JsonCreator
    public JobParams( @JsonProperty( "infraParams" ) InfraParams infraParams,
                      @JsonProperty( "benchmarkingRun" ) BenchmarkingRun<P> benchmarkingRun )
    {
        this.infraParams = infraParams;
        this.benchmarkingRun = benchmarkingRun;
    }

    public InfraParams infraParams()
    {
        return infraParams;
    }

    public BenchmarkingRun<P> benchmarkingRun()
    {
        return benchmarkingRun;
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
