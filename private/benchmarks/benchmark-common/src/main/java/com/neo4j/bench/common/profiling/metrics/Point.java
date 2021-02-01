/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.metrics;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.time.Instant;

public class Point
{
    private final Instant timestamp;
    private final double value;

    public Point( double value, Instant timestamp )
    {
        this.timestamp = timestamp;
        this.value = value;
    }

    public Instant timestamp()
    {
        return timestamp;
    }

    public double value()
    {
        return value;
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
        return timestamp + "=" + value;
    }
}
