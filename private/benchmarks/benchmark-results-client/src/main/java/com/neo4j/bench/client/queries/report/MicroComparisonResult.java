/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.report;

import java.text.DecimalFormat;
import java.util.Objects;

public class MicroComparisonResult extends CsvRow
{
    static final String HEADER = "Suite,Benchmark,Old,New,Unit,Difference";
    private static DecimalFormat NUMBER = new DecimalFormat( "#.00" );

    private final String group;
    private final String benchSimple;
    private final String benchFull;
    private final double oldResult;
    private final double newResult;
    private final String unit;
    private final double improvement;

    MicroComparisonResult( String group,
                           String benchSimple,
                           String benchFull,
                           double oldResult,
                           double newResult,
                           String unit,
                           double improvement )
    {
        this.group = group;
        this.benchSimple = benchSimple;
        this.benchFull = benchFull;
        this.oldResult = oldResult;
        this.newResult = newResult;
        this.unit = unit;
        this.improvement = improvement;
    }

    public String group()
    {
        return group;
    }

    public String benchSimple()
    {
        return benchSimple;
    }

    public String benchFull()
    {
        return benchFull;
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
               Objects.equals( benchSimple, that.benchSimple ) &&
               Objects.equals( benchFull, that.benchFull ) &&
               Objects.equals( unit, that.unit );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( group, benchSimple, benchFull, oldResult, newResult, unit, improvement );
    }

    @Override
    public String toString()
    {
        return row();
    }

    @Override
    protected String[] unescapedRow()
    {
        return new String[]{group,
                            benchFull.replace( ",", ":" ),
                            NUMBER.format( oldResult ),
                            NUMBER.format( newResult ),
                            unit,
                            NUMBER.format( improvement )};
    }
}
