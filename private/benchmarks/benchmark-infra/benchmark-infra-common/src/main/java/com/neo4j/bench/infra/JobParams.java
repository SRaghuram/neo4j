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

public class JobParams
{

    private final InfraParams infraParams;
    private final BenchmarkingEnvironment benchmarkingEnvironment;

    @JsonCreator
    public JobParams( @JsonProperty( "infraParams" ) InfraParams infraParams,
                      @JsonProperty( "benchmarkingEnvironment" ) BenchmarkingEnvironment benchmarkingEnvironment )
    {
        this.infraParams = infraParams;
        this.benchmarkingEnvironment = benchmarkingEnvironment;
    }

    public InfraParams infraParams()
    {
        return infraParams;
    }

    public BenchmarkingEnvironment benchmarkingEnvironment()
    {
        return benchmarkingEnvironment;
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
