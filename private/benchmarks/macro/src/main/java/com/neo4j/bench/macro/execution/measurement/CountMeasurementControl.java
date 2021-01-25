/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class CountMeasurementControl implements MeasurementControl
{
    private final long maxCount;
    private long count;

    CountMeasurementControl( long maxCount )
    {
        this.maxCount = maxCount;
        this.count = 0;
    }

    @Override
    public void register( double measurement )
    {
        count++;
    }

    @Override
    public boolean isComplete()
    {
        return count >= maxCount;
    }

    @Override
    public void reset()
    {
        count = 0;
    }

    @Override
    public String description()
    {
        return "count( " + maxCount + " )";
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public boolean equals( Object obj )
    {
        return EqualsBuilder.reflectionEquals( this, obj );
    }

    @Override
    public String toString()
    {
        return description();
    }
}
