/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

public class Layout
{
    @JsonProperty( "title" )
    private final String title;
    @JsonProperty( "xaxis" )
    private final Axis xaxis;
    @JsonProperty( "yaxis" )
    private final Axis yaxis;

    public Layout( String title, Axis yaxis )
    {
        this.title = title;
        this.xaxis = new Axis( "Time" );
        this.yaxis = yaxis;
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
        return ToStringBuilder.reflectionToString( this, SHORT_PREFIX_STYLE );
    }
}
