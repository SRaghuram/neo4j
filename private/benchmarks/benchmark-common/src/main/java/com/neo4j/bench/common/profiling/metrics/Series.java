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

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

public class Series
{
    @JsonProperty( "name" )
    private final String name;
    @JsonProperty( "x" )
    private final List<Long> x;
    @JsonProperty( "y" )
    private final List<Double> y;

    public static Series toPlotlyChart( String name, List<Point> points )
    {
        List<Long> x = new ArrayList<>( points.size() );
        List<Double> y = new ArrayList<>( points.size() );
        for ( Point point : points )
        {
            x.add( point.timestamp().toEpochMilli() );
            y.add( point.value() );
        }
        return new Series( name, x, y );
    }

    public Series( String name, List<Long> x, List<Double> y )
    {
        this.name = name;
        this.x = x;
        this.y = y;
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
