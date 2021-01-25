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

import java.util.Objects;

public class RunToolMacroWorkloadParams
{
    private final RunMacroWorkloadParams runMacroWorkloadParams;
    private final String storeName;

    @JsonCreator
    public RunToolMacroWorkloadParams( @JsonProperty( "runMacroWorkloadParams" ) RunMacroWorkloadParams runMacroWorkloadParams,
                                       @JsonProperty( "storeName" ) String storeName )
    {
        Objects.requireNonNull( runMacroWorkloadParams );
        Objects.requireNonNull( storeName );
        this.runMacroWorkloadParams = runMacroWorkloadParams;
        this.storeName = storeName;
    }

    public RunMacroWorkloadParams runMacroWorkloadParams()
    {
        return runMacroWorkloadParams;
    }

    public String storeName()
    {
        return storeName;
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
