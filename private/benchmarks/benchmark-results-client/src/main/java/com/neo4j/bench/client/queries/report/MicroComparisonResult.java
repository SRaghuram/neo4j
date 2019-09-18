/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.report;

import java.text.DecimalFormat;
import java.util.Objects;

import static java.lang.String.format;

public class MicroComparisonResult
{
    private static DecimalFormat NUMBER = new DecimalFormat( "#.00" );

    private final String group;
    private final String bench;
    private final double oldResult;
    private final double newResult;
    private final String unit;
    private final double improvement;

    MicroComparisonResult( String group, String bench, double oldResult, double newResult, String unit, double improvement )
    {
        this.group = group;
        this.bench = bench;
        this.oldResult = oldResult;
        this.newResult = newResult;
        this.unit = unit;
        this.improvement = improvement;
    }

    public String group()
    {
        return group;
    }

    public String bench()
    {
        return bench;
    }

    public double oldResult()
    {
        return oldResult;
    }

    public double newResult()
    {
        return newResult;
    }

    public String unit()
    {
        return unit;
    }

    public double improvement()
    {
        return improvement;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        MicroComparisonResult that = (MicroComparisonResult) o;
        return Double.compare( that.oldResult, oldResult ) == 0 &&
               Double.compare( that.newResult, newResult ) == 0 &&
               Double.compare( that.improvement, improvement ) == 0 &&
               Objects.equals( group, that.group ) &&
               Objects.equals( bench, that.bench ) &&
               Objects.equals( unit, that.unit );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( group, bench, oldResult, newResult, unit, improvement );
    }

    @Override
    public String toString()
    {
        return format( "%s, %s, %s, %s, %s, %s",
                       group,
                       bench,
                       NUMBER.format( oldResult ),
                       NUMBER.format( newResult ),
                       unit,
                       NUMBER.format( improvement ) + "x" );
    }
}
