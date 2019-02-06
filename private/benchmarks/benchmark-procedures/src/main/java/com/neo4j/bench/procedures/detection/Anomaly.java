/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.detection;

import java.text.DecimalFormat;
import java.util.Objects;

public class Anomaly
{
    public enum AnomalyType
    {
        NONE,
        REGRESSION,
        IMPROVEMENT
    }

    private static final DecimalFormat CHANGE = new DecimalFormat( "#,###,##0.00" );

    private final Point prev;
    private final Point point;
    private final AnomalyType anomalyType;
    private final double change;

    Anomaly( Point prev, Point point, AnomalyType anomalyType, double change )
    {
        this.prev = prev;
        this.point = point;
        this.anomalyType = anomalyType;
        this.change = change;
    }

    Point prev()
    {
        return prev;
    }

    Point point()
    {
        return point;
    }

    AnomalyType type()
    {
        return anomalyType;
    }

    double change()
    {
        return change;
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
        Anomaly anomaly = (Anomaly) o;
        return Double.compare( anomaly.change, change ) == 0 &&
               Objects.equals( prev, anomaly.prev ) &&
               Objects.equals( point, anomaly.point ) &&
               anomalyType == anomaly.anomalyType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( prev, point, anomalyType, change );
    }

    @Override
    public String toString()
    {
        return "(" + prev + " --> " + point + "," + anomalyType + "," + CHANGE.format( change ) + "x)";
    }
}
