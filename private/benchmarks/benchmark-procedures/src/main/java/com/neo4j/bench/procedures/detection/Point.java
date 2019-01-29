package com.neo4j.bench.procedures.detection;

import com.neo4j.bench.client.Units;
import com.neo4j.bench.client.model.Neo4j;
import com.neo4j.bench.client.model.Benchmark.Mode;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class Point
{
    static final ValueComparator BY_VALUE = new ValueComparator();
    static final DateComparator BY_DATE = new DateComparator();
    private static final SimpleDateFormat DATE = new SimpleDateFormat( "YYYY/MM/dd - hh:mm:ss" );
    private static final DecimalFormat NUMBER = new DecimalFormat( "#,###,##0.0000000000" );

    private final long metricsNodeId;
    private final long date;
    private final double value;
    private final TimeUnit unit;
    private final Neo4j neo4j;

    public Point( long metricsNodeId, long date, double value, TimeUnit unit, Neo4j neo4j )
    {
        this.metricsNodeId = metricsNodeId;
        this.date = date;
        this.value = value;
        this.unit = unit;
        this.neo4j = neo4j;
    }

    // conversion is necessary because TimeUnit convert can only deal with long values
    static double convertTo( double fromValue, TimeUnit fromUnit, TimeUnit toUnit, Mode mode )
    {
        return fromValue * Units.conversionFactor( fromUnit, toUnit, mode );
    }

    long nodeId()
    {
        return metricsNodeId;
    }

    long date()
    {
        return date;
    }

    double value()
    {
        return value;
    }

    TimeUnit unit()
    {
        return unit;
    }

    Neo4j neo4j()
    {
        return neo4j;
    }

    Point convertTo( TimeUnit toUnit, Mode mode )
    {
        double convertedValue = value * Units.conversionFactor( unit, toUnit, mode );
        return new Point( metricsNodeId, date, convertedValue, toUnit, neo4j );
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
        Point point = (Point) o;
        return metricsNodeId == point.metricsNodeId &&
               date == point.date &&
               Double.compare( point.value, value ) == 0 &&
               unit == point.unit &&
               Objects.equals( neo4j, point.neo4j );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( metricsNodeId, date, value, unit, neo4j );
    }

    @Override
    public String toString()
    {
        return format( "Point(%s,%s,%s,%s)",
                       DATE.format( new Date( date ) ), NUMBER.format( value ), unit, neo4j.version() );
    }

    private static class ValueComparator implements Comparator<Point>
    {
        @Override
        public int compare( Point o1, Point o2 )
        {
            return Double.compare( o1.value(), o2.value() );
        }
    }

    private static class DateComparator implements Comparator<Point>
    {
        @Override
        public int compare( Point o1, Point o2 )
        {
            return Long.compare( o1.date(), o2.date() );
        }
    }
}
