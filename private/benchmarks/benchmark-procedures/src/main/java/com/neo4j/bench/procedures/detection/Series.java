/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.detection;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.Benchmark.Mode;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.client.Units.conversionFactor;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class Series
{
    private static final List<TimeUnit> UNITS = Lists.newArrayList( NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS );

    private final List<Point> points;
    private final String neo4jSeries;
    private final Mode mode;

    static TimeUnit saneUnitFor( Series series )
    {
        Point max = series.convertTo( NANOSECONDS ).max();
        int index = UNITS.indexOf( max.unit() );
        double factor = conversionFactor( max.unit(), UNITS.get( index ), series.mode() );
        while ( index < UNITS.size() - 1 && (max.value() * factor >= 1000 || max.value() * factor < 1) )
        {
            factor = conversionFactor( max.unit(), UNITS.get( ++index ), series.mode() );
        }
        return UNITS.get( index );
    }

    public Series( String neo4jSeries, List<Point> points, Mode mode )
    {
        this.points = points;
        this.neo4jSeries = neo4jSeries;
        this.mode = mode;
    }

    Series convertTo( TimeUnit to )
    {
        return new Series( neo4jSeries, points.stream().map( p -> p.convertTo( to, mode ) ).collect( toList() ), mode );
    }

    Point max()
    {
        return points.stream()
                     .max( Point.BY_VALUE )
                     .orElseThrow( () -> new RuntimeException( "No point found" ) );
    }

    int size()
    {
        return points.size();
    }

    List<Point> points( Comparator<Point> comparator )
    {
        return points.stream().sorted( comparator ).collect( toList() );
    }

    String neo4jSeries()
    {
        return neo4jSeries;
    }

    Mode mode()
    {
        return mode;
    }
}
