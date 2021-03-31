/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.macro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class RunToolMacroWorkloadParams
{
    private final RunMacroWorkloadParams runMacroWorkloadParams;
    private final URI dataSetUri;

    @JsonCreator
    public RunToolMacroWorkloadParams( @JsonProperty( "runMacroWorkloadParams" ) RunMacroWorkloadParams runMacroWorkloadParams,
                                       @JsonProperty( "dataSetUri" ) URI dataSetUri )
    {
        this.runMacroWorkloadParams = requireNonNull( runMacroWorkloadParams );
        this.dataSetUri = requireNonNull( dataSetUri );
    }

    public RunMacroWorkloadParams runMacroWorkloadParams()
    {
        return runMacroWorkloadParams;
    }

    public URI dataSetUri()
    {
        return dataSetUri;
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

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }
}
