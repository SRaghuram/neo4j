/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

abstract class CompositeMeasurementControl implements MeasurementControl
{
    final List<MeasurementControl> measurementControls;

    CompositeMeasurementControl( MeasurementControl... measurementControls )
    {
        if ( measurementControls.length < 1 )
        {
            throw new RuntimeException( "Expected at least one measurement control" );
        }
        this.measurementControls = Lists.newArrayList( measurementControls );
    }

    @Override
    public void register( long latency )
    {
        measurementControls.forEach( measurementControl -> measurementControl.register( latency ) );
    }

    @Override
    public void reset()
    {
        measurementControls.forEach( MeasurementControl::reset );
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
