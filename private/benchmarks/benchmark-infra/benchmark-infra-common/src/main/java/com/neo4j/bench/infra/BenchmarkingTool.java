/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static java.lang.String.format;

/**
 * Holds type and configuration of benchmarking tool (like micro, macro etc.)
 */
public class BenchmarkingTool<P>
{

    private final Class<? extends BenchmarkingToolRunner<P>> toolRunnerClass;
    private final P toolParameters;

    @JsonCreator
    public BenchmarkingTool(
            @JsonProperty( "toolRunnerClass" ) Class<? extends BenchmarkingToolRunner<P>> toolRunnerClass,
            @JsonProperty( "toolParameters" ) P toolParameters )
    {
        this.toolRunnerClass = toolRunnerClass;
        this.toolParameters = toolParameters;
    }

    public Class<? extends BenchmarkingToolRunner> toolRunnerClass()
    {
        return toolRunnerClass;
    }

    @JsonTypeInfo( use = JsonTypeInfo.Id.CLASS )
    public P getToolParameters()
    {
        return toolParameters;
    }

    public BenchmarkingToolRunner newRunner()
    {
        try
        {
            return toolRunnerClass.getConstructor().newInstance();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "unable to call no-args constructor of class %s", toolRunnerClass ), e );
        }
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
