/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static java.util.stream.Collectors.joining;

public class AndCompositeMeasurementControl extends CompositeMeasurementControl
{
    AndCompositeMeasurementControl( MeasurementControl... measurementControls )
    {
        super( measurementControls );
    }

    @Override
    public boolean isComplete()
    {
        return measurementControls.stream().allMatch( MeasurementControl::isComplete );
    }

    @Override
    public String description()
    {
        return "and( " + measurementControls.stream().map( MeasurementControl::description ).collect( joining( " , " ) ) + " )";
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
}
